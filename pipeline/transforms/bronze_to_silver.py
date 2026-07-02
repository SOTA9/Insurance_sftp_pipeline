from __future__ import annotations

import hashlib
import logging
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Optional

import pandas as pd

from pipeline.state import SilverState

logger = logging.getLogger(__name__)

SNAPPY = "snappy"
SILVER_PREFIX = "silver"   # gs://bucket/silver/{entity}/year=.../...


# GCS helpers

def _gcs_client(project: str, sa_key_path: Optional[Path] = None):
    from google.cloud import storage
    if sa_key_path:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(str(sa_key_path))
        return storage.Client(project=project, credentials=creds)
    return storage.Client(project=project)


def _list_bronze_dates_for_entity(bucket, entity: str) -> list[str]:
    """
    Scan ALL bronze prefixes for an entity across both directories.
    bronze/inbound/policies/year=.../month=.../day=.../
    bronze/reports/premiums/year=.../month=.../day=.../
    Returns sorted list of ISO date strings.
    """
    dates: set[str] = set()
    # Scan every prefix that contains this entity
    for blob in bucket.list_blobs(prefix="bronze/"):
        # blob.name example:
        #   bronze/inbound/policies/year=2026/month=05/day=14/policies_20260514.csv
        parts = blob.name.split("/")
        # We need: bronze / {dir} / {entity} / year=... / month=... / day=...
        if len(parts) < 7:
            continue
        if parts[2] != entity:
            continue
        try:
            year  = int([p for p in parts if p.startswith("year=")][0].split("=")[1])
            month = int([p for p in parts if p.startswith("month=")][0].split("=")[1])
            day   = int([p for p in parts if p.startswith("day=")][0].split("=")[1])
            dates.add(date(year, month, day).isoformat())
        except (IndexError, ValueError):
            pass
    return sorted(dates)


def _list_bronze_blobs_for_entity_date(
    bucket, entity: str, business_date: date
) -> list:
    """
    Find all bronze CSV blobs for an entity on a specific date,
    across ALL directory prefixes (inbound, reports, etc.).
    Returns list of GCS blob objects.
    """
    blobs = []
    year  = business_date.year
    month = business_date.month
    day   = business_date.day

    # Scan bronze/ — matches both bronze/inbound/ and bronze/reports/
    for blob in bucket.list_blobs(prefix="bronze/"):
        parts = blob.name.split("/")
        if len(parts) < 7:
            continue
        if parts[2] != entity:
            continue
        if not blob.name.endswith(".csv"):
            continue
        # Check date partition
        try:
            b_year  = int([p for p in parts if p.startswith("year=")][0].split("=")[1])
            b_month = int([p for p in parts if p.startswith("month=")][0].split("=")[1])
            b_day   = int([p for p in parts if p.startswith("day=")][0].split("=")[1])
            if (b_year, b_month, b_day) == (year, month, day):
                blobs.append(blob)
        except (IndexError, ValueError):
            pass

    return blobs


def _read_csv_from_blob(blob) -> pd.DataFrame:
    content = blob.download_as_bytes()
    logger.info("Read bronze: %s (%d bytes)", blob.name, len(content))
    return pd.read_csv(BytesIO(content), dtype=str, keep_default_na=False)


def _write_parquet(df: pd.DataFrame, bucket, entity: str,
                   business_date: date) -> str:
    silver_blob_name = (
        f"{SILVER_PREFIX}/{entity}/"
        f"year={business_date.year}/"
        f"month={business_date.month:02d}/"
        f"day={business_date.day:02d}/"
        f"{entity}_{business_date.strftime('%Y%m%d')}.parquet"
    )
    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression=SNAPPY, index=False)
    buf.seek(0)
    bucket.blob(silver_blob_name).upload_from_file(
        buf, content_type="application/octet-stream"
    )
    uri = f"gs://{bucket.name}/{silver_blob_name}"
    logger.info("Written silver Parquet: %s (%d rows)", uri, len(df))
    return uri


# Normalisation helpers

def _row_hash(df: pd.DataFrame, key_cols: list[str]) -> pd.Series:
    combined = df[key_cols].astype(str).agg("|".join, axis=1)
    return combined.apply(lambda s: hashlib.sha256(s.encode()).hexdigest())


def _add_metadata(df: pd.DataFrame, source_file: str,
                  business_date: date) -> pd.DataFrame:
    df["_ingested_at"]   = pd.Timestamp.utcnow()
    df["_source_file"]   = source_file
    df["_business_date"] = pd.Timestamp(business_date)
    return df


def _to_date(series: pd.Series, col: str) -> pd.Series:
    coerced = pd.to_datetime(series, errors="coerce", utc=False)
    bad = coerced.isna() & series.notna() & (series != "")
    if bad.any():
        logger.warning("Column '%s': %d values not parseable as date → NaT.", col, bad.sum())
    return coerced


def _to_float(series: pd.Series, col: str) -> pd.Series:
    coerced = pd.to_numeric(series.str.strip(), errors="coerce")
    bad = coerced.isna() & series.notna() & (series != "")
    if bad.any():
        logger.warning("Column '%s': %d non-numeric values → NaN.", col, bad.sum())
    return coerced


def _to_bool(series: pd.Series) -> pd.Series:
    return series.str.lower().map(
        {"true": True, "false": False, "1": True, "0": False,
         "yes": True, "no": False}
    )


def _strip_nulls(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(
        {"": None, "None": None, "nan": None, "NaN": None, "NULL": None}
    )


def _deduplicate(df: pd.DataFrame, key: str) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=[key], keep="last")
    dropped = before - len(df)
    if dropped:
        logger.warning("Deduplicated %d rows on key='%s'.", dropped, key)
    return df


# Entity transformers

class PoliciesTransformer:
    NATURAL_KEY   = "policy_id"
    MONETARY_COLS = ["premium_amount"]
    DATE_COLS     = ["policy_start_date", "policy_end_date"]

    def transform(self, df: pd.DataFrame, source_file: str,
                  business_date: date) -> pd.DataFrame:
        df = _strip_nulls(df.copy())
        df["policy_id"] = df["policy_id"].str.upper().str.strip()
        for col in self.DATE_COLS:
            df[col] = _to_date(df[col], col)
        for col in self.MONETARY_COLS:
            df[col] = _to_float(df[col], col)
        for col in ["coverage_type", "status"]:
            df[col] = df[col].str.upper().str.strip()
        df["policy_duration_days"] = (
            (df["policy_end_date"] - df["policy_start_date"]).dt.days
        )
        df["annual_premium_eur"] = df["premium_amount"]
        df["risk_score"] = _to_float(
            df["risk_score"], "risk_score"
        ).clip(0.0, 1.0)
        df = _deduplicate(df, self.NATURAL_KEY)
        df["_row_hash"] = _row_hash(df, [self.NATURAL_KEY])
        return _add_metadata(df, source_file, business_date)


class ClaimsTransformer:
    NATURAL_KEY   = "claim_id"
    MONETARY_COLS = ["claimed_amount", "approved_amount", "deductible_applied"]
    DATE_COLS     = ["incident_date", "reported_date"]

    def transform(self, df: pd.DataFrame, source_file: str,
                  business_date: date) -> pd.DataFrame:
        df = _strip_nulls(df.copy())
        for col in ["claim_id", "policy_id"]:
            df[col] = df[col].str.upper().str.strip()
        for col in self.DATE_COLS:
            df[col] = _to_date(df[col], col)
        for col in self.MONETARY_COLS:
            df[col] = _to_float(df[col], col)
        df["days_to_report"] = (
            (df["reported_date"] - df["incident_date"]).dt.days
        ).clip(lower=0)
        mask = df["claimed_amount"] > 0
        df["approval_rate"] = None
        df.loc[mask, "approval_rate"] = (
            df.loc[mask, "approved_amount"]
            / df.loc[mask, "claimed_amount"]
        ).clip(0.0, 1.0)
        for col in ["claim_type", "status"]:
            df[col] = df[col].str.upper().str.strip()
        df["fraud_flag"] = _to_bool(df["fraud_flag"].astype(str))
        df = _deduplicate(df, self.NATURAL_KEY)
        df["_row_hash"] = _row_hash(df, [self.NATURAL_KEY])
        return _add_metadata(df, source_file, business_date)


class PremiumsTransformer:
    NATURAL_KEY   = "premium_id"
    MONETARY_COLS = ["amount_due", "amount_paid", "late_fee"]
    DATE_COLS     = ["payment_date", "due_date"]

    def transform(self, df: pd.DataFrame, source_file: str,
                  business_date: date) -> pd.DataFrame:
        df = _strip_nulls(df.copy())
        for col in ["premium_id", "policy_id"]:
            df[col] = df[col].str.upper().str.strip()
        for col in self.DATE_COLS:
            df[col] = _to_date(df[col], col)
        for col in self.MONETARY_COLS:
            df[col] = _to_float(df[col], col)
        df["payment_variance"] = df["amount_paid"] - df["amount_due"]
        now_ts = pd.Timestamp(date.today())
        df["days_overdue"] = (
            (now_ts - df["due_date"]).dt.days
            .where(df["status"] == "OVERDUE", other=None)
        )
        df["currency"] = df["currency"].str.upper().str.strip()
        df["status"]   = df["status"].str.upper().str.strip()
        df["instalment_number"] = pd.to_numeric(
            df["instalment_number"], errors="coerce"
        ).astype("Int64")
        df["instalment_total"] = pd.to_numeric(
            df["instalment_total"], errors="coerce"
        ).astype("Int64")
        df = _deduplicate(df, self.NATURAL_KEY)
        df["_row_hash"] = _row_hash(df, [self.NATURAL_KEY])
        return _add_metadata(df, source_file, business_date)


class ReinsuranceTransformer:
    NATURAL_KEY   = "ri_record_id"
    MONETARY_COLS = ["ceded_premium", "ceded_liability", "recovered_amount"]
    DATE_COLS     = ["cession_date"]

    def transform(self, df: pd.DataFrame, source_file: str,
                  business_date: date) -> pd.DataFrame:
        df = _strip_nulls(df.copy())
        for col in ["ri_record_id", "policy_id"]:
            df[col] = df[col].str.upper().str.strip()
        for col in self.DATE_COLS:
            df[col] = _to_date(df[col], col)
        for col in self.MONETARY_COLS:
            df[col] = _to_float(df[col], col)
        df["cession_percentage"] = _to_float(
            df["cession_percentage"], "cession_percentage"
        ).clip(0.0, 100.0)
        df["retained_liability"] = df["ceded_liability"] * (
            1 - df["cession_percentage"] / 100
        )
        mask = df["ceded_liability"] > 0
        df["recovery_ratio"] = None
        df.loc[mask, "recovery_ratio"] = (
            df.loc[mask, "recovered_amount"]
            / df.loc[mask, "ceded_liability"]
        ).clip(0.0, 1.0)
        df["currency"]    = df["currency"].str.upper().str.strip()
        df["treaty_type"] = df["treaty_type"].str.upper().str.strip()
        df["status"]      = df["status"].str.upper().str.strip()
        df = _deduplicate(df, self.NATURAL_KEY)
        df["_row_hash"] = _row_hash(df, [self.NATURAL_KEY])
        return _add_metadata(df, source_file, business_date)


TRANSFORMERS: dict[str, object] = {
    "policies":    PoliciesTransformer(),
    "claims":      ClaimsTransformer(),
    "premiums":    PremiumsTransformer(),
    "reinsurance": ReinsuranceTransformer(),
}


# Main entry points

def bronze_to_silver_incremental(
    entity: str,
    business_date: date,
    gcs_bucket: str,
    gcs_project: str,
    force_reprocess: bool = False,
    sa_key_path: Optional[Path] = None,
) -> Optional[str]:
    """
    Transform bronze to silver for one entity on one business date.

    Incremental behaviour:
      - Loads SilverState for this entity.
      - If business_date already in state AND force_reprocess=False → skip.
      - Otherwise: read all bronze CSVs for entity+date → transform →
        deduplicate across files → write Parquet → update state.

    Returns:
      GCS URI of written Parquet, or None if skipped.

    force_reprocess=True:
      Re-transform even if already in state. Use after fixing a transformer bug.
    """
    if entity not in TRANSFORMERS:
        raise ValueError(
            f"Unknown entity: {entity!r}. Known: {list(TRANSFORMERS)}"
        )

    client = _gcs_client(gcs_project, sa_key_path)
    bucket = client.bucket(gcs_bucket)
    state  = SilverState(gcs_bucket, entity, gcs_project, sa_key_path)
    date_str = business_date.isoformat()

    # Incremental check
    if state.is_processed(date_str) and not force_reprocess:
        logger.info(
            "[silver/%s] %s already processed — skipping (use force_reprocess=True to override).",
            entity, date_str,
        )
        return None

    if force_reprocess and state.is_processed(date_str):
        logger.info(
            "[silver/%s] force_reprocess=True — re-transforming %s.", entity, date_str
        )

    # Find bronze CSVs for this entity + date across all directories
    blobs = _list_bronze_blobs_for_entity_date(bucket, entity, business_date)

    if not blobs:
        logger.warning(
            "[silver/%s] No bronze CSVs found for %s — nothing to transform.",
            entity, date_str,
        )
        return None

    logger.info(
        "[silver/%s] Found %d bronze file(s) for %s: %s",
        entity, len(blobs), date_str, [b.name for b in blobs],
    )

    # Transform each file
    transformer = TRANSFORMERS[entity]
    frames = []
    for blob in blobs:
        df_raw   = _read_csv_from_blob(blob)
        df_clean = transformer.transform(df_raw, blob.name, business_date)
        frames.append(df_clean)
        logger.info(
            "[silver/%s] Transformed %s → %d rows.", entity, blob.name, len(df_clean)
        )

    # Merge multiple files + cross-file dedup
    if len(frames) > 1:
        df_final = pd.concat(frames, ignore_index=True)
        nat_key  = TRANSFORMERS[entity].NATURAL_KEY
        before   = len(df_final)
        df_final = df_final.drop_duplicates(subset=[nat_key], keep="last")
        if before != len(df_final):
            logger.warning(
                "[silver/%s] Cross-file dedup: %d → %d rows.", entity, before, len(df_final)
            )
    else:
        df_final = frames[0]

    # Write Parquet to silver
    uri = _write_parquet(df_final, bucket, entity, business_date)

    # Update state AFTER successful write
    state.mark_processed(date_str)
    state.save()

    logger.info(
        "[silver/%s] Complete: date=%s rows=%d uri=%s",
        entity, date_str, len(df_final), uri,
    )
    return uri


def bronze_to_silver_backfill(
    entity: str,
    gcs_bucket: str,
    gcs_project: str,
    force_reprocess: bool = False,
    sa_key_path: Optional[Path] = None,
) -> dict[str, Optional[str]]:
    """
    Process ALL available bronze dates for one entity.

    Use this:
      - On first setup to transform all historical bronze data.
      - After a transformer bug fix to rewrite all silver for an entity.
      - After resetting SilverState via state.reset().

    Returns {date_str: gcs_uri_or_None} for every date attempted.
    """
    client = _gcs_client(gcs_project, sa_key_path)
    bucket = client.bucket(gcs_bucket)
    state  = SilverState(gcs_bucket, entity, gcs_project, sa_key_path)

    all_dates = _list_bronze_dates_for_entity(bucket, entity)
    dates_to_process = state.get_unprocessed_dates(all_dates, force_reprocess)

    logger.info(
        "[silver/%s] Backfill: %d dates to process (of %d total bronze dates).",
        entity, len(dates_to_process), len(all_dates),
    )

    results: dict[str, Optional[str]] = {}
    for date_str in dates_to_process:
        bd = date.fromisoformat(date_str)
        try:
            uri = bronze_to_silver_incremental(
                entity, bd, gcs_bucket, gcs_project,
                force_reprocess, sa_key_path,
            )
            results[date_str] = uri
        except Exception as exc:
            logger.error("[silver/%s] Backfill failed for %s: %s", entity, date_str, exc)
            results[date_str] = None

    return results