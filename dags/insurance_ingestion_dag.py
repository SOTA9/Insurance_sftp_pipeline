#   1. make_directory_group() factory creates one TaskGroup per SFTP directory
#      (inbound + reports). Groups run IN PARALLEL.
#   2. check_and_diff task loads SFTPState and returns ONLY new files.
#      First run = all files. Later runs = delta only.
#   3. decrypt task calls pgp_handler.process_directory() which handles
#      both use_pgp=True (inbound) and use_pgp=False (reports) internally.
#   4. checksum task calls validate_directory() which skips when use_checksum=False.
#   5. upload_bronze task commits SFTPState AFTER successful GCS upload.
#   6. transform_to_silver task loads SilverState per entity — skips done dates.
#   7. load_to_gold task loads GoldState per fact table — DELETE+INSERT per date.
#   8. Dimensions + aggregates always WRITE_TRUNCATE (no state needed).

from __future__ import annotations

import logging
import tempfile
from datetime import date, timedelta
from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago

from pipeline.config import settings
from pipeline.directory_config import directory_config, SFTPDirectory
from pipeline.sftp_client import SFTPClient
from pipeline.pgp_handler import PGPHandler
from pipeline.checksum_validator import validate_directory
from pipeline.file_arrival_checker import FileArrivalChecker
from pipeline.data_validator import validate_file
from pipeline.gcs_uploader import GCSUploader
from pipeline.audit_logger import write_audit
from pipeline.alerting import notify_slack
from pipeline.state import SFTPState, SilverState, GoldState
from pipeline.transforms.bronze_to_silver import bronze_to_silver_incremental
from pipeline.transforms.silver_to_gold import silver_to_gold
from dags.dag_utils import on_failure_callback, on_sla_miss_callback, log_pipeline_banner

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": [settings.ALERT_EMAIL],
    "on_failure_callback": on_failure_callback,
    "sla": timedelta(hours=2),
}


# TaskGroup factory: one group per SFTP directory

def make_directory_group(sftp_dir: SFTPDirectory):
    """
    Returns a callable TaskGroup for one SFTP directory.
    Called once per entry in manifests/sftp_directories.yaml.

    /inbound/  : group_id="ingest_inbound"  (PGP encrypted, checksums)
    /reports/  : group_id="ingest_reports"  (plain CSV, no checksums)
    """

    @task_group(group_id=f"ingest_{sftp_dir.dir_id}")
    def directory_group(**context):

        # Step 1: List SFTP dir + compute incremental delta
        @task(task_id=f"check_{sftp_dir.dir_id}")
        def check_and_diff(**ctx) -> dict:
            """
            Lists all files on the SFTP directory.
            Loads SFTPState to find which files were already processed.
            Returns new_files = sftp_listing minus already_processed.

            First run:  state is empty -> new_files = everything on SFTP -> full load.
            Later runs: new_files = only files not yet in state → incremental.
            """
            bd = date.fromisoformat(ctx["ds"])
            log_pipeline_banner(ctx["dag"].dag_id, bd)

            # Load state for this directory
            state = SFTPState(
                gcs_bucket=settings.GCS_BUCKET,
                state_key=sftp_dir.state_key,
                gcs_project=settings.GCS_PROJECT,
                sa_key_path=settings.GCS_SA_KEY_PATH,
            )

            # List current SFTP contents
            tmp = Path(tempfile.mkdtemp())
            with SFTPClient(download_dir=tmp) as sftp:
                all_sftp_files = sftp.list_remote_files(sftp_dir.remote_dir)

            # Compute delta
            new_files = state.get_new_files(all_sftp_files)

            # Run arrival check only on new files (verifies today's expected files present)
            if new_files:
                FileArrivalChecker().check(
                    sftp_dir=sftp_dir,
                    business_date=bd,
                    actual_files=new_files,
                )

            logger.info(
                "[%s] first_run=%s total_sftp=%d new=%d",
                sftp_dir.dir_id, state.is_first_run, len(all_sftp_files), len(new_files)
            )
            return {
                "dir_id":        sftp_dir.dir_id,
                "remote_dir":    sftp_dir.remote_dir,
                "business_date": bd.isoformat(),
                "new_files":     new_files,
                "has_new_files": len(new_files) > 0,
                "is_first_run":  state.is_first_run,
            }

        # Step 2: Download new files from SFTP
        @task(task_id=f"download_{sftp_dir.dir_id}")
        def download(check_result: dict) -> dict:
            """
            Downloads only the files identified as new by check_and_diff.
            If has_new_files=False: short-circuits, no SFTP connection opened.

            Files are stored under /tmp/dl_{dir_id}/{dir_slug}/{filename}
            The dir_slug namespace prevents filename collisions between directories.
            e.g /tmp/dl_inbound/inbound/policies_20260514.csv.gpg
                 /tmp/dl_reports/reports/premiums_20260514.csv
            """
            if not check_result["has_new_files"]:
                logger.info("[%s] No new files — download skipped.", sftp_dir.dir_id)
                return {
                    **check_result,
                    "download_dir":     "",
                    "downloaded_files": [],
                    "skipped":          True,
                }

            tmp_dir = Path(tempfile.mkdtemp(prefix=f"dl_{sftp_dir.dir_id}_"))
            with SFTPClient(download_dir=tmp_dir) as sftp:
                pairs = sftp.download_files(
                    remote_dir=sftp_dir.remote_dir,
                    filenames=check_result["new_files"],
                )
                # Archive on SFTP: moves file to processed/ subfolder
                # Provides a second layer of protection against re-ingestion
                for fn, _ in pairs:
                    sftp.archive_remote_file(sftp_dir.remote_dir, fn)

            return {
                **check_result,
                "download_dir":     str(tmp_dir),
                "downloaded_files": [fn for fn, _ in pairs],
                "skipped":          False,
            }

        # Step 3: Decrypt or passthrough
        @task(task_id=f"decrypt_{sftp_dir.dir_id}")
        def decrypt(dl_result: dict) -> dict:
            """
            Calls pgp_handler.process_directory() which handles both cases:
              use_pgp=True  (/inbound/)  → decrypt .gpg files using company private key
              use_pgp=False (/reports/)  → copy plain CSV files as-is to decrypt_dir

            Output always lands in {download_dir}/decrypted/ as plain .csv files.
            This makes the downstream tasks (checksum, validate, upload) identical
            regardless of whether the files were encrypted or not.
            """
            if dl_result.get("skipped"):
                return {**dl_result, "decrypt_dir": ""}

            dl_dir      = Path(dl_result["download_dir"])
            decrypt_dir = dl_dir / "decrypted"

            PGPHandler(dir_id=sftp_dir.dir_id).process_directory(
                use_pgp=sftp_dir.use_pgp,
                download_dir=dl_dir,
                decrypt_dir=decrypt_dir,
                downloaded_files=dl_result["downloaded_files"],
            )
            return {**dl_result, "decrypt_dir": str(decrypt_dir)}

        # Step 4: Checksum verification
        @task(task_id=f"checksum_{sftp_dir.dir_id}")
        def checksum(dec_result: dict) -> dict:
            """
            use_checksum=True  (/inbound/):  verify SHA-256 of each CSV against sidecar.
            use_checksum=False (/reports/):  skip entirely — no sidecars provided.
            Raises RuntimeError on any mismatch (corrupted or tampered file).
            """
            if dec_result.get("skipped") or not dec_result.get("decrypt_dir"):
                return dec_result

            validate_directory(
                data_dir=Path(dec_result["decrypt_dir"]),
                use_checksum=sftp_dir.use_checksum,
                checksum_ext=settings.CHECKSUM_EXTENSION,
            )
            return dec_result

        # Step 5: Schema validation
        @task(task_id=f"validate_{sftp_dir.dir_id}")
        def validate(chk_result: dict) -> dict:
            """
            Runs Pandera schema validation on every CSV in decrypt_dir.
            Failed files : dead-letter GCS prefix (not silently dropped).
            Passing files : continue to bronze upload.
            """
            if chk_result.get("skipped") or not chk_result.get("decrypt_dir"):
                return {**chk_result, "valid_files": [], "stats": []}

            dec_dir     = Path(chk_result["decrypt_dir"])
            bd          = date.fromisoformat(chk_result["business_date"])
            uploader    = GCSUploader()
            valid_files = []
            stats_list  = []

            for csv_path in dec_dir.glob("*.csv"):
                try:
                    _, stats = validate_file(csv_path, sftp_dir)
                    valid_files.append(str(csv_path))
                    stats_list.append(stats)
                    logger.info("[%s] Valid: %s (%d rows)",
                                sftp_dir.dir_id, csv_path.name, stats["rows_valid"])
                except Exception as exc:
                    logger.error("[%s] INVALID %s: %s", sftp_dir.dir_id, csv_path.name, exc)
                    entity = uploader._infer_entity(csv_path.name, sftp_dir) or "unknown"
                    uploader.upload_dead_letter(csv_path, sftp_dir.dir_id, entity, bd)

            return {**chk_result, "valid_files": valid_files, "stats": stats_list}

        # Step 6: Upload to GCS bronze + commit state
        @task(task_id=f"bronze_{sftp_dir.dir_id}")
        def upload_bronze(val_result: dict) -> dict:
            """
            Uploads validated CSVs to GCS bronze prefix.

            GCS paths (from SFTPDirectory.gcs_entity_prefix):
              bronze/inbound/policies/year=2026/month=05/day=14/policies_20260514.csv
              bronze/reports/premiums/year=2026/month=05/day=14/premiums_20260514.csv

            STATE IS COMMITTED ONLY AFTER SUCCESSFUL UPLOAD.
            If upload fails: files are NOT in state → next DAG run retries them.
            This is the at-least-once guarantee.
            """
            if val_result.get("skipped") or not val_result.get("valid_files"):
                logger.info("[%s] No files to upload to bronze.", sftp_dir.dir_id)
                return {
                    "dir_id":        sftp_dir.dir_id,
                    "bronze_uris":   [],
                    "stats":         [],
                    "business_date": val_result["business_date"],
                }

            bd       = date.fromisoformat(val_result["business_date"])
            uploader = GCSUploader()
            uris     = uploader.upload_for_directory(
                sftp_dir=sftp_dir,
                valid_csv_paths=[Path(p) for p in val_result["valid_files"]],
                business_date=bd,
            )

            # Commit state after upload success
            state = SFTPState(
                gcs_bucket=settings.GCS_BUCKET,
                state_key=sftp_dir.state_key,
                gcs_project=settings.GCS_PROJECT,
                sa_key_path=settings.GCS_SA_KEY_PATH,
            )
            fn_to_uri: dict[str, str] = {}
            for csv_name, uri in uris.items():
                fn_to_uri[csv_name] = uri
                if sftp_dir.use_pgp:
                    fn_to_uri[csv_name + ".gpg"] = uri  # mark original encrypted file too

            state.mark_batch(list(fn_to_uri.keys()), bd.isoformat(), fn_to_uri)
            state.save()
            logger.info("[%s] State committed: %d files.", sftp_dir.dir_id, len(fn_to_uri))

            return {
                "dir_id":        sftp_dir.dir_id,
                "bronze_uris":   list(uris.values()),
                "stats":         val_result.get("stats", []),
                "business_date": val_result["business_date"],
            }

        # Wire tasks inside this group
        check_xcom = check_and_diff()
        dl_xcom    = download(check_xcom)
        dec_xcom   = decrypt(dl_xcom)
        chk_xcom   = checksum(dec_xcom)
        val_xcom   = validate(chk_xcom)
        return upload_bronze(val_xcom)

    return directory_group


# Main DAG

@dag(
    dag_id="insurance_sftp_gcs_ingestion",
    default_args=DEFAULT_ARGS,
    description=(
        "Multi-directory SFTP ingestion with incremental Bronze -> Silver -> Gold loading. "
        "Two directories: /inbound/ (PGP+checksum) and /reports/ (plain CSV)."
    ),
    schedule="0 7 * * 1-5",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=on_sla_miss_callback,
    tags=["insurance", "ingestion", "sftp", "incremental", "multi-dir"],
)
def insurance_ingestion_dag():

    # One TaskGroup per directory: run in parallel
    directory_results = []
    for sftp_dir in directory_config.all():
        group_fn = make_directory_group(sftp_dir)
        directory_results.append(group_fn())

    # Silver: incremental transform : waits for ALL groups
    @task(task_id="transform_to_silver")
    def transform_to_silver(*bronze_results, **context) -> dict:
        """
        Transform bronze CSVs to silver Parquet for every entity.
        Waits for both ingest_inbound and ingest_reports to complete.

        Per entity SilverState check:
          Already in state for this date : skip (idempotent).
          Not in state → run transformer : write Parquet -> mark state.

        First run:  all entities transformed for all available bronze dates.
        Later runs: only entities with new bronze data for this date.
        """
        bd = date.fromisoformat(context["ds"])
        silver_uris: dict[str, str] = {}

        for entity in directory_config.all_entities():
            silver_state = SilverState(
                gcs_bucket=settings.GCS_BUCKET,
                entity=entity,
                gcs_project=settings.GCS_PROJECT,
                sa_key_path=settings.GCS_SA_KEY_PATH,
            )

            if silver_state.is_processed(bd.isoformat()):
                logger.info("[silver/%s] %s already transformed — skipping.", entity, bd)
                silver_uris[entity] = "skipped"
                continue

            try:
                uri = bronze_to_silver_incremental(
                    entity=entity,
                    business_date=bd,
                    gcs_bucket=settings.GCS_BUCKET,
                    gcs_project=settings.GCS_PROJECT,
                    sa_key_path=settings.GCS_SA_KEY_PATH,
                )
                if uri:
                    silver_state.mark_processed(bd.isoformat())
                    silver_state.save()
                    silver_uris[entity] = uri
                    logger.info("[silver/%s] Done: %s", entity, uri)
                else:
                    logger.info("[silver/%s] No bronze data for %s.", entity, bd)
                    silver_uris[entity] = "no_bronze"
            except Exception as exc:
                logger.error("[silver/%s] FAILED for %s: %s", entity, bd, exc)
                silver_uris[entity] = f"error: {exc}"

        all_bronze_uris, all_stats = [], []
        for r in bronze_results:
            all_bronze_uris.extend(r.get("bronze_uris", []))
            all_stats.extend(r.get("stats", []))

        return {
            "business_date": bd.isoformat(),
            "silver_uris":   silver_uris,
            "bronze_uris":   all_bronze_uris,
            "stats":         all_stats,
        }

    # Gold: incremental load
    @task(task_id="load_to_gold")
    def load_to_gold(silver_result: dict) -> dict:
        """
        Loads silver Parquet to BigQuery gold layer.

        Facts (fact_premiums, fact_claims, fact_reinsurance):
          GoldState check: already loaded for this date → skip.
          Not loaded -> DELETE existing partition -> INSERT new rows -> mark state.
          This DELETE+INSERT pattern is idempotent: safe to re-run.

        Dimensions (dim_policy, dim_claimant, dim_reinsurer):
          Always WRITE_TRUNCATE — no state needed, full refresh is fast.

        Aggregates (agg_*):
          Always WRITE_TRUNCATE — recomputed fresh every run.
        """
        bd = date.fromisoformat(silver_result["business_date"])
        gold_results = silver_to_gold(
            business_date=bd,
            gcs_bucket=settings.GCS_BUCKET,
            gcs_project=settings.GCS_PROJECT,
            bq_project=settings.GCS_PROJECT,
            sa_key_path=settings.GCS_SA_KEY_PATH,
        )
        return {**silver_result, "gold_results": gold_results}

    # Audit
    @task(task_id="write_audit_record")
    def write_audit_record(gold_result: dict) -> None:
        write_audit(
            business_date=gold_result["business_date"],
            gcs_uris=gold_result["bronze_uris"],
            stats=gold_result["stats"],
        )

    # Notify
    @task(task_id="notify_success")
    def notify_success(gold_result: dict) -> None:
        n_dirs   = len(directory_config.all())
        n_bronze = len(gold_result["bronze_uris"])
        n_silver = len([v for v in gold_result["silver_uris"].values()
                        if not v.startswith(("skipped", "no_bronze", "error"))])
        n_gold   = len([v for v in gold_result.get("gold_results", {}).values()
                        if v == "ok"])
        notify_slack(
            f":white_check_mark: InsureFlow complete — {gold_result['business_date']}\n"
            f"Dirs: {n_dirs} | Bronze: {n_bronze} files | "
            f"Silver: {n_silver} entities | Gold: {n_gold} tables"
        )

    # Wire: directory groups -> silver -> gold -> audit -> notify
    # *directory_results unpacks the list so transform_to_silver only starts
    # after ALL directory groups have completed (both inbound + reports)
    silver_xcom = transform_to_silver(*directory_results)
    gold_xcom   = load_to_gold(silver_xcom)
    write_audit_record(gold_xcom)
    notify_success(gold_xcom)


dag_instance = insurance_ingestion_dag()