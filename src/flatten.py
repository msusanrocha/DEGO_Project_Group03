from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

KNOWN_TOP_LEVEL_FIELDS = {
    "_id",
    "processing_timestamp",
    "applicant_info",
    "financials",
    "decision",
    "spending_behavior",
}


def _safe_dict(value: Any) -> dict[str, Any]:
    """Return the input if it is a dict, else an empty dict."""
    return value if isinstance(value, dict) else {}


def _optional_top_level_fields(records: Iterable[dict[str, Any]]) -> list[str]:
    """Collect non-standard top-level JSON keys for pass-through flattening."""
    fields: set[str] = set()
    for record in records:
        fields.update(record.keys())
    return sorted(fields - KNOWN_TOP_LEVEL_FIELDS)


def flatten_applications(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Flatten raw records into one application row per source JSON entry."""
    optional_fields = _optional_top_level_fields(records)
    rows: list[dict[str, Any]] = []
    for row_id, record in enumerate(records):
        applicant = _safe_dict(record.get("applicant_info"))
        financials = _safe_dict(record.get("financials"))
        decision = _safe_dict(record.get("decision"))

        row: dict[str, Any] = {
            "application_row_id": row_id,
            "application_id": record.get("_id"),
            "raw_processing_timestamp": record.get("processing_timestamp"),
            "raw_applicant_full_name": applicant.get("full_name"),
            "raw_applicant_email": applicant.get("email"),
            "raw_applicant_ssn": applicant.get("ssn"),
            "raw_applicant_ip_address": applicant.get("ip_address"),
            "raw_applicant_gender": applicant.get("gender"),
            "raw_applicant_date_of_birth": applicant.get("date_of_birth"),
            "raw_applicant_zip_code": applicant.get("zip_code"),
            "raw_financial_annual_income": financials.get("annual_income"),
            "raw_financial_annual_salary": financials.get("annual_salary"),
            "raw_financial_credit_history_months": financials.get("credit_history_months"),
            "raw_financial_debt_to_income": financials.get("debt_to_income"),
            "raw_financial_savings_balance": financials.get("savings_balance"),
            "raw_decision_loan_approved": decision.get("loan_approved"),
            "raw_decision_interest_rate": decision.get("interest_rate"),
            "raw_decision_approved_amount": decision.get("approved_amount"),
            "raw_decision_rejection_reason": decision.get("rejection_reason"),
        }
        for field in optional_fields:
            row[f"raw_{field}"] = record.get(field)
        rows.append(row)

    df = pd.DataFrame(rows)
    return df.sort_values("application_row_id").reset_index(drop=True)


def flatten_spending_items(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Explode spending_behavior into one row per spending item."""
    rows: list[dict[str, Any]] = []
    for row_id, record in enumerate(records):
        application_id = record.get("_id")
        spending_items = record.get("spending_behavior")
        if not isinstance(spending_items, list):
            continue
        for idx, item in enumerate(spending_items):
            spending = _safe_dict(item)
            rows.append(
                {
                    "application_row_id": row_id,
                    "application_id": application_id,
                    "spending_index": idx,
                    "raw_category": spending.get("category"),
                    "raw_amount": spending.get("amount"),
                }
            )
    df = pd.DataFrame(
        rows,
        columns=[
            "application_row_id",
            "application_id",
            "spending_index",
            "raw_category",
            "raw_amount",
        ],
    )
    if df.empty:
        return df
    return df.sort_values(["application_row_id", "spending_index"]).reset_index(drop=True)
