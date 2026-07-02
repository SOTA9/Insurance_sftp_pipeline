import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

reinsurance_schema = DataFrameSchema(
    columns={
        "ri_record_id": Column(
            str,
            checks=[
                Check.str_matches(r"^RI-[0-9]{8}$"),
                Check.not_null(),
            ],
            nullable=False,
            unique=True,
            description="Unique reinsurance record identifier e.g. RI-20260001",
        ),
        "policy_id": Column(
            str,
            checks=[
                Check.str_matches(r"^POL-[0-9]{8}$"),
                Check.not_null(),
            ],
            nullable=False,
        ),
        "claim_id": Column(
            str,
            checks=Check.str_matches(r"^CLM-[0-9]{8}$"),
            nullable=True,
        ),
        "reinsurer_id": Column(
            str,
            checks=Check.str_matches(r"^RNS-[A-Z0-9]{6}$"),
            nullable=False,
        ),
        "reinsurer_name": Column(str, nullable=False),
        "treaty_id":      Column(str, nullable=False),
        "treaty_type": Column(
            str,
            checks=Check.isin([
                "QUOTA_SHARE", "SURPLUS", "EXCESS_OF_LOSS",
                "STOP_LOSS", "FACULTATIVE",
            ]),
            nullable=False,
        ),
        "cession_date": Column(
            pa.DateTime,
            nullable=False,
            checks=Check(
                lambda s: s >= pd.Timestamp("2000-01-01"),
                error="cession_date must be >= 2000-01-01",
            ),
        ),
        "cession_percentage": Column(
            float,
            checks=Check.in_range(0.0, 100.0),
            nullable=False,
            description="Percentage of risk ceded to reinsurer",
        ),
        "ceded_premium": Column(
            float,
            checks=Check.greater_than_or_equal_to(0),
            nullable=False,
        ),
        "ceded_liability": Column(
            float,
            checks=Check.greater_than_or_equal_to(0),
            nullable=False,
        ),
        "recovered_amount": Column(
            float,
            checks=Check.greater_than_or_equal_to(0),
            nullable=True,
        ),
        "currency": Column(
            str,
            checks=Check.str_matches(r"^[A-Z]{3}$"),
            nullable=False,
        ),
        "status": Column(
            str,
            checks=Check.isin([
                "ACTIVE", "SETTLED", "DISPUTED",
                "CANCELLED", "PENDING_RECOVERY",
            ]),
            nullable=False,
        ),
        # ADDED: records which SFTP directory produced this row
        # reinsurance comes from /reports/ (use_pgp=False directory)
        "dir_id": Column(
            str,
            nullable=True,
            required=False,
            description="SFTP source directory e.g. 'reports'",
        ),
    },
    index=pa.Index(int),
    strict=False,
    coerce=True,
    name="ReinsuranceSchema",
)