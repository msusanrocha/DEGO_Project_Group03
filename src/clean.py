from __future__ import annotations

import re
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from . import config


def _blank_mask(series: pd.Series) -> pd.Series:
    """Return a boolean mask for null or blank string-like values."""
    as_obj = series.astype("object")
    return as_obj.isna() | as_obj.astype(str).str.strip().eq("")


def _to_numeric(series: pd.Series) -> pd.Series:
    """Coerce a pandas Series to numeric with invalid values set to NaN."""
    return pd.to_numeric(series, errors="coerce")


def _to_bool(series: pd.Series) -> pd.Series:
    """Coerce mixed truthy and falsey values into a nullable boolean-like Series."""
    mapping = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
    }
    out: list[Any] = []
    for value in series:
        if isinstance(value, bool):
            out.append(value)
        elif value is None or (isinstance(value, float) and np.isnan(value)):
            out.append(np.nan)
        else:
            out.append(mapping.get(str(value).strip().lower(), np.nan))
    return pd.Series(out, index=series.index, dtype="object")


def _parse_dob_value(value: Any) -> tuple[pd.Timestamp | pd.NaT, bool, bool]:
    """Return parsed DOB, ambiguity flag, and parse-failed flag for one source value."""
    if value is None:
        return pd.NaT, False, False
    value_str = str(value).strip()
    if value_str == "":
        return pd.NaT, False, False

    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            parsed = pd.to_datetime(value_str, format=fmt, errors="raise")
            return pd.Timestamp(parsed), False, False
        except ValueError:
            continue

    if re.match(r"^\d{2}/\d{2}/\d{4}$", value_str):
        left, right, year_str = value_str.split("/")
        left_num = int(left)
        right_num = int(right)
        year_num = int(year_str)
        ambiguous = left_num <= 12 and right_num <= 12
        if left_num > 12:
            day, month = left_num, right_num
        else:
            month, day = left_num, right_num
        try:
            parsed_date = date(year_num, month, day)
            return pd.Timestamp(parsed_date), ambiguous, False
        except ValueError:
            return pd.NaT, False, True

    return pd.NaT, False, True


def _normalise_text(series: pd.Series, lower: bool = False) -> pd.Series:
    """Trim whitespace, optionally lowercase, and convert blanks to missing values."""
    cleaned = series.fillna("").astype(str).str.strip()
    if lower:
        cleaned = cleaned.str.lower()
    return cleaned.mask(cleaned.eq(""), pd.NA)


def clean_applications(applications_df: pd.DataFrame) -> pd.DataFrame:
    """Apply deterministic cleaning and keep only audit-relevant remediation flags."""
    out = applications_df.copy().sort_values("application_row_id").reset_index(drop=True)

    out["clean_email"] = _normalise_text(out["raw_applicant_email"], lower=True)

    gender_key = out["raw_applicant_gender"].fillna("").astype(str).str.strip().str.lower()
    out["clean_gender"] = gender_key.map(config.GENDER_MAP)
    out["gender_standardized_flag"] = gender_key.isin({"m", "f"})

    parsed_dob = out["raw_applicant_date_of_birth"].apply(_parse_dob_value)
    out["clean_date_of_birth"] = parsed_dob.apply(
        lambda item: item[0].strftime("%Y-%m-%d") if pd.notna(item[0]) else pd.NA
    )
    out["dob_ambiguous_flag"] = parsed_dob.apply(lambda item: bool(item[1]))
    out["dob_parse_failed_flag"] = parsed_dob.apply(lambda item: bool(item[2]))

    parsed_ts = pd.to_datetime(out["raw_processing_timestamp"], errors="coerce", utc=True)
    out["clean_processing_timestamp"] = parsed_ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    out["clean_zip_code"] = _normalise_text(out["raw_applicant_zip_code"], lower=False)

    income_raw = out["raw_financial_annual_income"]
    salary_raw = out["raw_financial_annual_salary"]
    income_missing = _blank_mask(income_raw)
    salary_missing = _blank_mask(salary_raw)
    income_selected = income_raw.where(~income_missing, salary_raw)
    out["annual_income_from_salary_flag"] = income_missing & (~salary_missing)
    out["clean_annual_income"] = _to_numeric(income_selected)

    credit_history_num = _to_numeric(out["raw_financial_credit_history_months"])
    out["credit_history_nullified_flag"] = credit_history_num < 0
    out["clean_credit_history_months"] = credit_history_num.mask(
        out["credit_history_nullified_flag"]
    ).round().astype("Int64")

    dti_num = _to_numeric(out["raw_financial_debt_to_income"])
    out["dti_nullified_flag"] = (dti_num < 0) | (dti_num > 1)
    out["clean_debt_to_income"] = dti_num.mask(out["dti_nullified_flag"])

    savings_num = _to_numeric(out["raw_financial_savings_balance"])
    out["savings_nullified_flag"] = savings_num < 0
    out["clean_savings_balance"] = savings_num.mask(out["savings_nullified_flag"])

    out["clean_loan_approved"] = _to_bool(out["raw_decision_loan_approved"])
    out["clean_interest_rate"] = _to_numeric(out["raw_decision_interest_rate"])
    out["clean_approved_amount"] = _to_numeric(out["raw_decision_approved_amount"])
    out["clean_rejection_reason"] = _normalise_text(out["raw_decision_rejection_reason"])

    approved_mask = out["clean_loan_approved"].eq(True)
    rejected_mask = out["clean_loan_approved"].eq(False)
    out["approved_missing_terms_flag"] = approved_mask & (
        out["clean_interest_rate"].isna() | out["clean_approved_amount"].isna()
    )
    out["rejected_missing_reason_flag"] = rejected_mask & out["clean_rejection_reason"].isna()

    return out


def clean_spending_items(spending_df: pd.DataFrame) -> pd.DataFrame:
    """Clean spending rows and keep only the minimal flag set needed downstream."""
    out = spending_df.copy().sort_values(["application_row_id", "spending_index"]).reset_index(drop=True)

    category_clean = out["raw_category"].fillna("").astype(str).str.strip()
    out["category_clean"] = category_clean.str.lower().str.title().mask(category_clean.eq(""), pd.NA)
    out["category_missing_flag"] = category_clean.eq("")

    amount_blank = _blank_mask(out["raw_amount"])
    amount_num = _to_numeric(out["raw_amount"])
    out["amount_invalid_flag"] = (~amount_blank) & amount_num.isna()
    out["amount_negative_flag"] = amount_num < 0
    out["amount_clean"] = amount_num.mask(out["amount_negative_flag"])

    return out
