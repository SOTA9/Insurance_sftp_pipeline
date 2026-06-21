"""
  1. Reads silver Parquet from gs://bucket/silver/{entity}/year=.../...
     (unchanged path — silver is entity-centric, not directory-centric)

  2. Uses GoldState to track which (fact_table, date) partitions are
     already loaded in BigQuery. Skips them on re-runs.

  3. Fact tables use DELETE-partition + INSERT for idempotency:
       - Delete WHERE ingestion_date = business_date
       - INSERT new rows (WRITE_APPEND)
     This means re-running for the same date is always safe.

  4. Dimension tables: always WRITE_TRUNCATE (small, idempotent).
  5. Aggregate tables: always WRITE_TRUNCATE (recomputed fresh each run).

GoldState blob locations:
  gs://bucket/_state/gold/fact_premiums/loaded_dates.json
  gs://bucket/_state/gold/fact_claims/loaded_dates.json
  gs://bucket/_state/gold/fact_reinsurance/loaded_dates.json
"""
from __future__ import annotations

import logging
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import bigquery, storage

from pipeline.state import GoldState

logger = logging.getLogger(__name__)

SILVER_PREFIX = "silver"

# BQ table names — must match table_id values in terraform/modules/bigquery/main.tf
TBL_DIM_POLICY       = "dim_policy"
TBL_DIM_CLAIMANT     = "dim_claimant"
TBL_DIM_REINSURER    = "dim_reinsurer"
TBL_FACT_PREMIUMS    = "fact_premiums"
TBL_FACT_CLAIMS      = "fact_claims"
TBL_FACT_REINSURANCE = "fact_reinsurance"
TBL_AGG_PREMIUM      = "agg_daily_premium_summary"
TBL_AGG_CLAIMS       = "agg_claim_by_status"
TBL_AGG_REINSURER    = "agg_reinsurer_exposure"

FACT_TABLES = [TBL_FACT_PREMIUMS, TBL_FACT_CLAIMS, TBL_FACT_REINSURANCE]


# GCS / BQ clients

def _bq_client(project: str, sa_key_path: Optional[Path] = None) -> bigquery.Client:
    if sa_key_path:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(str(sa_key_path))
        return bigquery.Client(project=project, credentials=creds)
    return bigquery.Client(project=project)


def _gcs_client(project: str, sa_key_path: Optional[Path] = None) -> storage.Client:
    if sa_key_path:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(str(sa_key_path))
        return storage.Client(project=project, credentials=creds)
    return storage.Client(project=project)


# Silver reader

def read_silver_parquet(
    gcs_bucket: str,
    entity: str,
    business_date: date,
    gcs_project: str,
    sa_key_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Read the silver Parquet for one entity and business date.
    Path: gs://bucket/silver/{entity}/year=.../month=.../day=.../
    Raises FileNotFoundError if no Parquet found (silver not yet written).
    """
    client = _gcs_client(gcs_project, sa_key_path)
    bucket = client.bucket(gcs_bucket)

    prefix = (
        f"{SILVER_PREFIX}/{entity}/"
        f"year={business_date.year}/"
        f"month={business_date.month:02d}/"
        f"day={business_date.day:02d}/"
    )
    blobs = [b for b in bucket.list_blobs(prefix=prefix)
             if b.name.endswith(".parquet")]

    if not blobs:
        raise FileNotFoundError(
            f"No silver Parquet for entity={entity} date={business_date}. "
            f"Run bronze_to_silver_incremental first."
        )

    frames = [
        pd.read_parquet(BytesIO(b.download_as_bytes()), engine="pyarrow")
        for b in blobs
    ]
    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    logger.info("[silver→gold] Read %s for %s: %d rows.", entity, business_date, len(df))
    return df


# BQ loaders

def _load_truncate(
    df: pd.DataFrame,
    project: str,
    dataset: str,
    table: str,
    clustering_fields: Optional[list[str]] = None,
    sa_key_path: Optional[Path] = None,
) -> None:
    """WRITE_TRUNCATE — used for dimensions and aggregates."""
    client     = _bq_client(project, sa_key_path)
    table_ref  = f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
    )
    if clustering_fields:
        job_config.clustering_fields = clustering_fields
    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
    logger.info("[gold] WRITE_TRUNCATE %s: %d rows.", table, len(df))


def _load_fact_incremental(
    df: pd.DataFrame,
    project: str,
    dataset: str,
    table: str,
    partition_field: str,
    business_date: date,
    clustering_fields: Optional[list[str]] = None,
    sa_key_path: Optional[Path] = None,
) -> None:
    """
    Idempotent fact load using DELETE partition + INSERT.

    Step 1: DELETE rows where DATE(partition_field) = business_date.
            Safe on first run (table may not exist yet — exception caught).
    Step 2: WRITE_APPEND new rows for this date.

    This guarantees: re-running the DAG for the same date never duplicates rows.
    """
    client    = _bq_client(project, sa_key_path)
    table_ref = f"{project}.{dataset}.{table}"
    date_str  = business_date.isoformat()

    # Step 1 — delete existing partition (idempotent)
    delete_sql = f"""
        DELETE FROM `{table_ref}`
        WHERE DATE({partition_field}) = DATE('{date_str}')
    """
    try:
        client.query(delete_sql).result()
        logger.info("[gold] Deleted %s partition %s.", table, date_str)
    except Exception as exc:
        # Table may not exist yet on first load — that is fine
        logger.info("[gold] Delete skipped for %s/%s: %s", table, date_str, exc)

    # Step 2 — append new rows
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
    )
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field,
    )
    if clustering_fields:
        job_config.clustering_fields = clustering_fields

    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
    logger.info("[gold] Loaded %s for %s: %d rows.", table, date_str, len(df))


# Dimension builders

def build_dim_policy(df: pd.DataFrame, business_date: date) -> pd.DataFrame:
    """
    dim_policy — SCD Type 1 (latest value wins).
    WRITE_TRUNCATE daily — no GoldState needed.
    Matches clustering = ["coverage_type", "status"] in terraform/modules/bigquery/main.tf
    """
    dim = df[[
        "policy_id", "insured_name", "coverage_type", "status",
        "policy_start_date", "policy_end_date", "policy_duration_days",
        "annual_premium_eur", "risk_score", "broker_id",
    ]].copy()
    dim["is_active"] = dim["status"] == "ACTIVE"
    dim["load_date"] = pd.Timestamp(business_date)
    return dim.drop_duplicates(subset=["policy_id"])


def build_dim_claimant(df: pd.DataFrame, business_date: date) -> pd.DataFrame:
    """
    dim_claimant — deduplicated claimant list.
    WRITE_TRUNCATE daily.
    """
    dim = df[["policy_id", "claimant_name", "adjuster_id"]].drop_duplicates(
        subset=["policy_id", "claimant_name"]
    ).copy()
    dim["claimant_sk"] = (
        (dim["policy_id"] + "|" + dim["claimant_name"])
        .apply(lambda s: abs(hash(s)) % (10**9))
    )
    dim["load_date"] = pd.Timestamp(business_date)
    return dim


def build_dim_reinsurer(df: pd.DataFrame, business_date: date) -> pd.DataFrame:
    """dim_reinsurer — one row per reinsurer. WRITE_TRUNCATE daily."""
    dim = df[["reinsurer_id", "reinsurer_name"]].drop_duplicates(
        subset=["reinsurer_id"]
    ).copy()
    dim["load_date"] = pd.Timestamp(business_date)
    return dim


# Fact builders

def build_fact_premiums(df: pd.DataFrame, business_date: date) -> pd.DataFrame:
    """
    fact_premiums — grain: 1 row per premium_id.
    PARTITION BY ingestion_date, CLUSTER BY policy_id, status.
    Matches terraform/modules/bigquery/main.tf fact_premiums table.
    """
    fact = df[[
        "premium_id", "policy_id", "due_date", "payment_date",
        "amount_due", "amount_paid", "late_fee", "payment_variance",
        "days_overdue", "instalment_number", "instalment_total",
        "payment_method", "currency", "status",
        "_business_date", "_row_hash",
    ]].copy()
    fact["ingestion_date"] = pd.Timestamp(business_date).date()
    fact["is_paid"]        = fact["status"] == "PAID"
    fact["is_overdue"]     = fact["status"] == "OVERDUE"
    return fact


def build_fact_claims(df: pd.DataFrame, business_date: date) -> pd.DataFrame:
    """
    fact_claims — grain: 1 row per claim_id.
    PARTITION BY ingestion_date, CLUSTER BY claim_type, status.
    """
    fact = df[[
        "claim_id", "policy_id", "incident_date", "reported_date",
        "claim_type", "status", "claimed_amount", "approved_amount",
        "deductible_applied", "days_to_report", "approval_rate",
        "fraud_flag", "adjuster_id",
        "_business_date", "_row_hash",
    ]].copy()
    fact["ingestion_date"] = pd.Timestamp(business_date).date()
    fact["is_fraud"]       = fact["fraud_flag"].astype(bool)
    fact["net_claim_cost"] = (
        fact["approved_amount"].fillna(0) - fact["deductible_applied"].fillna(0)
    )
    return fact


def build_fact_reinsurance(df: pd.DataFrame, business_date: date) -> pd.DataFrame:
    """
    fact_reinsurance — grain: 1 row per ri_record_id.
    PARTITION BY ingestion_date, CLUSTER BY reinsurer_id, treaty_type.
    """
    fact = df[[
        "ri_record_id", "policy_id", "claim_id",
        "reinsurer_id", "treaty_id", "treaty_type",
        "cession_date", "cession_percentage",
        "ceded_premium", "ceded_liability",
        "recovered_amount", "retained_liability", "recovery_ratio",
        "currency", "status",
        "_business_date", "_row_hash",
    ]].copy()
    fact["ingestion_date"] = pd.Timestamp(business_date).date()
    return fact


# Aggregate builders

def build_agg_daily_premium_summary(
    df: pd.DataFrame, business_date: date
) -> pd.DataFrame:
    """
    agg_daily_premium_summary — recomputed fresh every run.
    WRITE_TRUNCATE — no GoldState needed.
    """
    for col in ["amount_due", "amount_paid"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    agg = (
        df.groupby(["status", "currency"], dropna=False)
        .agg(
            policy_count     =("policy_id",    "nunique"),
            premium_count    =("premium_id",   "count"),
            total_amount_due =("amount_due",   "sum"),
            total_amount_paid=("amount_paid",  "sum"),
        )
        .reset_index()
    )
    safe_due = agg["total_amount_due"].replace(0, float("nan"))
    agg["collection_rate"] = (agg["total_amount_paid"] / safe_due).clip(0, 1)
    agg["business_date"]   = pd.Timestamp(business_date).date()
    agg["overdue_count"]   = (
        (agg["status"] == "OVERDUE").astype(int) * agg["premium_count"]
    )
    return agg


def build_agg_claim_by_status(
    df: pd.DataFrame, business_date: date
) -> pd.DataFrame:
    """agg_claim_by_status — recomputed fresh, WRITE_TRUNCATE."""
    for col in ["claimed_amount", "approved_amount", "days_to_report"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["fraud_flag"] = df["fraud_flag"].astype(str).str.lower() == "true"
    agg = (
        df.groupby(["claim_type", "status"], dropna=False)
        .agg(
            claim_count        =("claim_id",       "count"),
            total_claimed_eur  =("claimed_amount",  "sum"),
            total_approved_eur =("approved_amount", "sum"),
            avg_days_to_report =("days_to_report",  "mean"),
            fraud_count        =("fraud_flag",      "sum"),
        )
        .reset_index()
    )
    agg["business_date"]      = pd.Timestamp(business_date).date()
    agg["avg_days_to_report"] = agg["avg_days_to_report"].round(1)
    return agg


def build_agg_reinsurer_exposure(
    df: pd.DataFrame, business_date: date
) -> pd.DataFrame:
    """agg_reinsurer_exposure — recomputed fresh, WRITE_TRUNCATE."""
    for col in ["ceded_premium", "ceded_liability", "retained_liability",
                "recovered_amount", "recovery_ratio"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    agg = (
        df.groupby(
            ["reinsurer_id", "reinsurer_name", "treaty_type", "currency"],
            dropna=False,
        )
        .agg(
            cession_count            =("ri_record_id",     "count"),
            total_ceded_premium_eur  =("ceded_premium",    "sum"),
            total_ceded_liability_eur=("ceded_liability",  "sum"),
            total_retained_liability =("retained_liability","sum"),
            total_recovered_eur      =("recovered_amount", "sum"),
            avg_recovery_ratio       =("recovery_ratio",   "mean"),
        )
        .reset_index()
    )
    agg["business_date"]      = pd.Timestamp(business_date).date()
    agg["avg_recovery_ratio"] = agg["avg_recovery_ratio"].round(4)
    return agg


# Main entry point

def silver_to_gold(
    business_date: date,
    gcs_bucket: str,
    gcs_project: str,
    bq_project: str,
    bq_gold_dataset: Optional[str] = None,
    force_reload: bool = False,
    sa_key_path: Optional[Path] = None,
) -> dict[str, str]:
    """
    Incrementally load all silver entities → BigQuery gold tables
    for one business_date.

    Dimensions:  WRITE_TRUNCATE daily — always refreshed, no state.
    Facts:       DELETE partition + INSERT — tracked by GoldState.
                 Skipped if already loaded (unless force_reload=True).
    Aggregates:  WRITE_TRUNCATE daily — always recomputed, no state.

    force_reload=True: force reload all fact partitions for this date.
    Use when you fix a silver transformer and need to rewrite gold facts.

    Returns {table_name: "ok" | "skipped" | "error: ..."} for each table.
    """
    from pipeline.config import settings
    dataset  = bq_gold_dataset or settings.BQ_GOLD_DATASET
    date_str = business_date.isoformat()
    results: dict[str, str] = {}

    gcs_cl = _gcs_client(gcs_project, sa_key_path)
    bucket = gcs_cl.bucket(gcs_bucket)

    # Read all four silver entities
    def _read(entity: str) -> pd.DataFrame:
        return read_silver_parquet(
            gcs_bucket, entity, business_date, gcs_project, sa_key_path
        )

    try:
        df_policies    = _read("policies")
        df_claims      = _read("claims")
        df_premiums    = _read("premiums")
        df_reinsurance = _read("reinsurance")
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Cannot load gold for {date_str} — silver not ready: {exc}"
        ) from exc

    # Dimensions - WRITE_TRUNCATE, no state
    dim_configs = [
        (TBL_DIM_POLICY,    build_dim_policy(df_policies, business_date),
         ["coverage_type", "status"]),
        (TBL_DIM_CLAIMANT,  build_dim_claimant(df_claims, business_date),
         ["policy_id"]),
        (TBL_DIM_REINSURER, build_dim_reinsurer(df_reinsurance, business_date),
         None),
    ]
    for table, df, cluster in dim_configs:
        try:
            _load_truncate(df, bq_project, dataset, table,
                           clustering_fields=cluster, sa_key_path=sa_key_path)
            results[table] = "ok"
        except Exception as exc:
            logger.error("[gold] Dim load failed %s: %s", table, exc)
            results[table] = f"error: {exc}"

    # Facts - DELETE+INSERT per date, tracked by GoldState
    fact_configs = [
        (TBL_FACT_PREMIUMS,    build_fact_premiums(df_premiums, business_date),
         "ingestion_date", ["policy_id", "status"]),
        (TBL_FACT_CLAIMS,      build_fact_claims(df_claims, business_date),
         "ingestion_date", ["claim_type", "status"]),
        (TBL_FACT_REINSURANCE, build_fact_reinsurance(df_reinsurance, business_date),
         "ingestion_date", ["reinsurer_id", "treaty_type"]),
    ]
    for table, df, partition, cluster in fact_configs:
        state = GoldState(gcs_bucket, table, gcs_project, sa_key_path)

        if not state.needs_load(date_str, force_reload):
            logger.info(
                "[gold] %s already loaded for %s — skipping "
                "(use force_reload=True to override).", table, date_str,
            )
            results[table] = "skipped"
            continue

        try:
            _load_fact_incremental(
                df, bq_project, dataset, table,
                partition_field=partition,
                business_date=business_date,
                clustering_fields=cluster,
                sa_key_path=sa_key_path,
            )
            state.mark_loaded(date_str)
            state.save()
            results[table] = "ok"
        except Exception as exc:
            logger.error("[gold] Fact load failed %s: %s", table, exc)
            results[table] = f"error: {exc}"

    # Aggregates - WRITE_TRUNCATE, no state
    agg_configs = [
        (TBL_AGG_PREMIUM,   build_agg_daily_premium_summary(df_premiums, business_date)),
        (TBL_AGG_CLAIMS,    build_agg_claim_by_status(df_claims, business_date)),
        (TBL_AGG_REINSURER, build_agg_reinsurer_exposure(df_reinsurance, business_date)),
    ]
    for table, df in agg_configs:
        try:
            _load_truncate(df, bq_project, dataset, table,
                           sa_key_path=sa_key_path)
            results[table] = "ok"
        except Exception as exc:
            logger.error("[gold] Agg load failed %s: %s", table, exc)
            results[table] = f"error: {exc}"

    # Summary
    errors   = [k for k, v in results.items() if v.startswith("error")]
    skipped  = [k for k, v in results.items() if v == "skipped"]
    ok_count = sum(1 for v in results.values() if v == "ok")

    logger.info(
        "[gold] Complete for %s: %d ok | %d skipped | %d errors.",
        date_str, ok_count, len(skipped), len(errors),
    )
    if errors:
        raise RuntimeError(f"Gold load failed for tables: {errors}")

    return results