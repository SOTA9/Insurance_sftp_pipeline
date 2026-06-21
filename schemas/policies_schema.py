#   One new column added: "dir_id" (nullable STRING)
#
#   Why: In the multi-directory setup, the same entity type (policies) could
#   theoretically arrive from different directories. The dir_id column records
#   which SFTP directory the file came from ("inbound" in this case) so that
#   if data quality issues appear you can filter by source directory in BQ.
#
#   strict=False means extra columns are allowed, so adding dir_id in the
#   transformer does not break validation. But we declare it here explicitly
#   so Pandera validates it when present.
#
#   All other column definitions are IDENTICAL to the original.

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

policies_schema = DataFrameSchema(
    columns={
        "policy_id": Column(
            str,
            checks=[
                Check.str_matches(r"^POL-[0-9]{8}$"),
                Check.not_null(),
            ],
            nullable=False,
            unique=True,
            description="Unique policy identifier e.g. POL-20260001",
        ),
        "insured_name": Column(str, nullable=False),
        "policy_start_date": Column(
            pa.DateTime,
            nullable=False,
            checks=Check(
                lambda s: s >= pd.Timestamp("2000-01-01"),
                error="policy_start_date must be >= 2000-01-01",
            ),
        ),
        "policy_end_date": Column(pa.DateTime, nullable=False),
        "premium_amount": Column(
            float,
            checks=[
                Check.greater_than(0),
                Check.less_than(10_000_000),
            ],
            nullable=False,
        ),
        "coverage_type": Column(
            str,
            checks=Check.isin(["LIFE", "HEALTH", "AUTO", "PROPERTY", "LIABILITY"]),
            nullable=False,
        ),
        "risk_score": Column(
            float,
            checks=[Check.in_range(0.0, 1.0)],
            nullable=True,
        ),
        "broker_id": Column(str, nullable=True),
        "status": Column(
            str,
            checks=Check.isin(["ACTIVE", "CANCELLED", "EXPIRED", "PENDING"]),
            nullable=False,
        ),
        # ADDED: records which SFTP directory this row came from
        # "inbound" for policies (use_pgp=True directory)
        # nullable=True so old files without this column still validate
        "dir_id": Column(
            str,
            nullable=True,
            required=False,
            description="SFTP source directory e.g. 'inbound'",
        ),
    },
    index=pa.Index(int),
    strict=False,   # allow extra columns added by transformers
    coerce=True,
    name="PoliciesSchema",
)