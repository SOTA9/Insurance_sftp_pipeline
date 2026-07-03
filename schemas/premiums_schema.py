import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

premiums_schema = DataFrameSchema(
    columns={
        "premium_id": Column(
            str,
            checks=[
                Check.str_matches(r"^PRM-[0-9]{8}$"),
                Check.not_null(),
            ],
            nullable=False,
            unique=True,
            description="Unique premium payment identifier e.g. PRM-20260001",
        ),
        "policy_id": Column(
            str,
            checks=[
                Check.str_matches(r"^POL-[0-9]{8}$"),
                Check.not_null(),
            ],
            nullable=False,
            description="Foreign key to policies file",
        ),
        "payment_date": Column(
            pa.DateTime,
            nullable=True,
            checks=Check(
                lambda s: s.dropna() >= pd.Timestamp("2000-01-01"),
                error="payment_date must be >= 2000-01-01",
            ),
        ),
        "due_date": Column(
            pa.DateTime,
            nullable=False,
            checks=Check(
                lambda s: s >= pd.Timestamp("2000-01-01"),
                error="due_date must be >= 2000-01-01",
            ),
        ),
        "amount_due": Column(
            float,
            checks=[
                Check.greater_than(0),
                Check.less_than(10_000_000),
            ],
            nullable=False,
        ),
        "amount_paid": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0),
                Check.less_than(10_000_000),
            ],
            nullable=True,
        ),
        "late_fee": Column(
            float,
            checks=Check.greater_than_or_equal_to(0),
            nullable=True,
        ),
        "payment_method": Column(
            str,
            checks=Check.isin([
                "BANK_TRANSFER", "CREDIT_CARD", "DEBIT_CARD",
                "CHEQUE", "DIRECT_DEBIT", "OTHER",
            ]),
            nullable=True,
        ),
        "currency": Column(
            str,
            checks=Check.str_matches(r"^[A-Z]{3}$"),
            nullable=False,
        ),
        "instalment_number": Column(
            int,
            checks=[
                Check.greater_than_or_equal_to(1),
                Check.less_than_or_equal_to(12),
            ],
            nullable=False,
        ),
        "instalment_total": Column(
            int,
            checks=[
                Check.greater_than_or_equal_to(1),
                Check.less_than_or_equal_to(12),
            ],
            nullable=False,
        ),
        "status": Column(
            str,
            checks=Check.isin([
                "PENDING", "PAID", "OVERDUE",
                "PARTIALLY_PAID", "WAIVED", "REFUNDED",
            ]),
            nullable=False,
        ),
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
    name="PremiumsSchema",
)