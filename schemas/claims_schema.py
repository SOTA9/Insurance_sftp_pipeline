# dir_id column added (nullable, required=False)
# All other columns IDENTICAL to original.
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

claims_schema = DataFrameSchema(
    columns={
        "claim_id": Column(
            str,
            checks=[Check.str_matches(r"^CLM-[0-9]{8}$"), Check.not_null()],
            nullable=False, unique=True,
            description="Unique claim identifier e.g. CLM-20260001",
        ),
        "policy_id": Column(
            str,
            checks=[Check.str_matches(r"^POL-[0-9]{8}$"), Check.not_null()],
            nullable=False,
            description="Foreign key to policies file",
        ),
        "claimant_name": Column(str, nullable=False),
        "adjuster_id":   Column(str, nullable=True),
        "incident_date": Column(
            pa.DateTime, nullable=False,
            checks=Check(lambda s: s >= pd.Timestamp("1990-01-01"),
                         error="incident_date must be >= 1990-01-01"),
        ),
        "reported_date": Column(
            pa.DateTime, nullable=False,
            checks=Check(lambda s: s >= pd.Timestamp("1990-01-01"),
                         error="reported_date must be >= 1990-01-01"),
        ),
        "claim_type": Column(
            str,
            checks=Check.isin([
                "ACCIDENT", "THEFT", "FIRE", "FLOOD",
                "LIABILITY", "MEDICAL", "DEATH", "OTHER",
            ]),
            nullable=False,
        ),
        "status": Column(
            str,
            checks=Check.isin([
                "OPEN", "IN_REVIEW", "APPROVED",
                "PARTIALLY_APPROVED", "DENIED", "CLOSED", "APPEALED",
            ]),
            nullable=False,
        ),
        "claimed_amount": Column(
            float,
            checks=[Check.greater_than_or_equal_to(0), Check.less_than(50_000_000)],
            nullable=False,
        ),
        "approved_amount": Column(
            float,
            checks=[Check.greater_than_or_equal_to(0), Check.less_than(50_000_000)],
            nullable=True,
        ),
        "deductible_applied": Column(
            float, checks=Check.greater_than_or_equal_to(0), nullable=True,
        ),
        "fraud_flag": Column(
            bool, nullable=True,
            description="True if flagged for potential fraud investigation",
        ),
        # ADDED: SFTP source directory — "inbound" for claims
        "dir_id": Column(
            str, nullable=True, required=False,
            description="SFTP source directory e.g. 'inbound'",
        ),
    },
    index=pa.Index(int),
    strict=False,
    coerce=True,
    name="ClaimsSchema",
)