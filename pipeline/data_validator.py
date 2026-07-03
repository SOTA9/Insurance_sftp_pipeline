from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import pandera as pa

from schemas.policies_schema    import policies_schema
from schemas.claims_schema      import claims_schema
from schemas.premiums_schema    import premiums_schema
from schemas.reinsurance_schema import reinsurance_schema

logger = logging.getLogger(__name__)

# Schema map - keyed by entity name, same as sftp_directories.yaml entity values
SCHEMA_MAP = {
    "policies":    policies_schema,
    "claims":      claims_schema,
    "premiums":    premiums_schema,
    "reinsurance": reinsurance_schema,
}


def infer_file_type(
    filename: str,
    sftp_dir=None,   # Optional[SFTPDirectory]  avoids circular import
) -> str:
    """
    Determine the entity/schema key for a given filename.

    UPDATED: tries sftp_dir.files first if provided.
    This uses the exact mapping defined in sftp_directories.yaml
    rather than guessing from filename prefix.

    Falls back to the original prefix-matching logic for backward
    compatibility and for tests that do not provide sftp_dir.

    Raises ValueError if no match found.
    """
    # Method 1: use SFTPDirectory file entries (preferred for multi-dir)
    if sftp_dir is not None:
        for file_entry in sftp_dir.files:
            if filename.lower().startswith(file_entry.entity):
                return file_entry.entity

    # Method 2: prefix match against SCHEMA_MAP (original fallback)
    for key in SCHEMA_MAP:
        if filename.lower().startswith(key):
            return key

    raise ValueError(
        f"Cannot determine file type for: '{filename}'. "
        f"Known entities: {list(SCHEMA_MAP.keys())}. "
        f"Check that the filename starts with one of these entity names, "
        f"or add the entity to sftp_directories.yaml."
    )


def validate_file(
    csv_path: Path,
    sftp_dir=None,   # Optional[SFTPDirectory]
) -> Tuple[pd.DataFrame, dict]:
    """
    Load a CSV file and validate it against the correct Pandera schema.

    UPDATED: passes sftp_dir to infer_file_type() so schema lookup
    uses directory context when available.

    Returns (validated_df, stats_dict).
    Raises pandera.errors.SchemaErrors on validation failure — caller
    catches this and routes to dead-letter GCS prefix.

    The validation logic itself is UNCHANGED. Only the schema-lookup
    call now accepts the optional sftp_dir.
    """
    file_type = infer_file_type(csv_path.name, sftp_dir)
    schema    = SCHEMA_MAP[file_type]

    logger.info("Loading %s (entity=%s)", csv_path.name, file_type)
    df_raw = pd.read_csv(csv_path, dtype=str)
    row_count_raw = len(df_raw)

    logger.info("Validating %s (%d rows)", csv_path.name, row_count_raw)
    try:
        df_valid = schema.validate(df_raw, lazy=True)  # lazy=collect all errors
    except pa.errors.SchemaErrors as exc:
        logger.error(
            "Schema validation FAILED for %s:\n%s",
            csv_path.name, exc.failure_cases.to_string(),
        )
        raise

    stats = {
        "file":         csv_path.name,
        "file_type":    file_type,
        "rows_raw":     row_count_raw,
        "rows_valid":   len(df_valid),
        "rows_dropped": row_count_raw - len(df_valid),
        "columns":      list(df_valid.columns),
    }
    logger.info(
        "Validation passed: %s | rows=%d dropped=%d",
        csv_path.name, len(df_valid), stats["rows_dropped"],
    )
    return df_valid, stats