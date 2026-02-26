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
    """Coerce mixed truthy/falsey values into a nullable boolean-like Series."""
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
    """
    Return parsed_dob, ambiguous_flag, parse_failed_flag.

    Ambiguous rule:
    - For NN/NN/YYYY where both NN <= 12, parse as MM/DD/YYYY.
    """
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
        if ambiguous:
            month, day = left_num, right_num
        elif left_num > 12:
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
    """Trim whitespace, optionally lowercase, and convert blanks to NA."""
    cleaned = series.fillna("").astype(str).str.strip()
    if lower:
        cleaned = cleaned.str.lower()
    return cleaned.mask(cleaned.eq(""), pd.NA)


def clean_applications(applications_df: pd.DataFrame) -> pd.DataFrame:
    """Apply deterministic, auditable cleaning rules to flattened applications."""
    out = applications_df.copy().sort_values("application_row_id").reset_index(drop=True)

    # Email
    email_raw = out["raw_applicant_email"]
    email_blank = _blank_mask(email_raw)
    out["clean_email"] = _normalise_text(email_raw, lower=True)
    out["blank_email_flag"] = email_blank
    email_re = re.compile(config.EMAIL_REGEX)
    email_clean = email_raw.fillna("").astype(str).str.strip()
    out["has_invalid_email"] = (~email_blank) & (~email_clean.str.match(email_re))

    # Gender
    gender_raw = out["raw_applicant_gender"]
    gender_blank = _blank_mask(gender_raw)
    gender_norm_key = gender_raw.fillna("").astype(str).str.strip().str.lower()
    out["clean_gender"] = gender_norm_key.map(config.GENDER_MAP)
    out["gender_missing_flag"] = gender_blank
    out["gender_invalid_flag"] = (~gender_blank) & out["clean_gender"].isna()
    out["gender_normalized_flag"] = gender_norm_key.isin({"m", "f"})

    # Date of birth
    dob_raw = out["raw_applicant_date_of_birth"]
    dob_blank = _blank_mask(dob_raw)
    parsed_dob = dob_raw.apply(_parse_dob_value)
    out["clean_date_of_birth"] = parsed_dob.apply(
        lambda x: x[0].strftime("%Y-%m-%d") if pd.notna(x[0]) else pd.NA
    )
    out["dob_ambiguous_flag"] = parsed_dob.apply(lambda x: bool(x[1]))
    out["dob_parse_failed_flag"] = parsed_dob.apply(lambda x: bool(x[2]))
    out["dob_missing_flag"] = dob_blank

    # Zip
    out["clean_zip_code"] = _normalise_text(out["raw_applicant_zip_code"], lower=False)
    out["zip_missing_flag"] = out["clean_zip_code"].isna()

    # Processing timestamp
    ts_blank = _blank_mask(out["raw_processing_timestamp"])
    parsed_ts = pd.to_datetime(out["raw_processing_timestamp"], errors="coerce", utc=True)
    out["clean_processing_timestamp"] = parsed_ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    out["processing_timestamp_parse_failed_flag"] = (~ts_blank) & parsed_ts.isna()

    # Annual income + field drift annual_salary -> annual_income.
    annual_income_raw = out["raw_financial_annual_income"]
    annual_salary_raw = out["raw_financial_annual_salary"]
    annual_income_blank = _blank_mask(annual_income_raw)
    annual_salary_blank = _blank_mask(annual_salary_raw)
    annual_income_selected = annual_income_raw.where(~annual_income_blank, annual_salary_raw)
    out["annual_income_from_salary_flag"] = annual_income_blank & (~annual_salary_blank)
    out["annual_income_source"] = np.where(
        ~annual_income_blank,
        "annual_income",
        np.where(~annual_salary_blank, "annual_salary", "missing"),
    )
    out["clean_annual_income"] = _to_numeric(annual_income_selected)
    out["annual_income_coerce_failed_flag"] = (~_blank_mask(annual_income_selected)) & out[
        "clean_annual_income"
    ].isna()
    out["annual_income_missing_flag"] = out["clean_annual_income"].isna()

    # Credit history months
    credit_history_raw = out["raw_financial_credit_history_months"]
    credit_history_blank = _blank_mask(credit_history_raw)
    credit_history_num = _to_numeric(credit_history_raw)
    out["credit_history_non_numeric_flag"] = (~credit_history_blank) & credit_history_num.isna()
    out["credit_history_negative_flag"] = credit_history_num < 0
    out["credit_history_nullified_flag"] = out["credit_history_negative_flag"]
    clean_credit_history = credit_history_num.mask(out["credit_history_negative_flag"])
    out["clean_credit_history_months"] = clean_credit_history.round().astype("Int64")

    # Debt to income
    dti_raw = out["raw_financial_debt_to_income"]
    dti_blank = _blank_mask(dti_raw)
    dti_num = _to_numeric(dti_raw)
    out["dti_non_numeric_flag"] = (~dti_blank) & dti_num.isna()
    out["dti_out_of_range_flag"] = (dti_num < 0) | (dti_num > 1)
    out["dti_nullified_flag"] = out["dti_out_of_range_flag"]
    out["clean_debt_to_income"] = dti_num.mask(out["dti_out_of_range_flag"])

    # Savings
    savings_raw = out["raw_financial_savings_balance"]
    savings_blank = _blank_mask(savings_raw)
    savings_num = _to_numeric(savings_raw)
    out["savings_non_numeric_flag"] = (~savings_blank) & savings_num.isna()
    out["savings_negative_flag"] = savings_num < 0
    out["savings_nullified_flag"] = out["savings_negative_flag"]
    out["clean_savings_balance"] = savings_num.mask(out["savings_negative_flag"])

    # Decision fields
    approved_bool = _to_bool(out["raw_decision_loan_approved"])
    out["clean_loan_approved"] = approved_bool
    out["clean_interest_rate"] = _to_numeric(out["raw_decision_interest_rate"])
    out["clean_approved_amount"] = _to_numeric(out["raw_decision_approved_amount"])
    out["clean_rejection_reason"] = _normalise_text(out["raw_decision_rejection_reason"], lower=False)

    approved_mask = approved_bool.eq(True)
    rejected_mask = approved_bool.eq(False)
    out["approved_missing_fields_flag"] = approved_mask & (
        out["clean_interest_rate"].isna() | out["clean_approved_amount"].isna()
    )
    out["rejected_missing_reason_flag"] = rejected_mask & out["clean_rejection_reason"].isna()

    credit_clean_numeric = pd.to_numeric(out["clean_credit_history_months"], errors="coerce")
    out["approved_with_zero_credit_history_flag"] = approved_mask & credit_clean_numeric.eq(0)
    out["approved_with_lt6_credit_history_flag"] = approved_mask & (credit_clean_numeric < 6)

    return out


def clean_spending_items(spending_df: pd.DataFrame) -> pd.DataFrame:
    """Clean spending item category/amount fields and attach remediation flags."""
    out = spending_df.copy().sort_values(["application_row_id", "spending_index"]).reset_index(drop=True)
    category_raw = out["raw_category"]
    category_clean = category_raw.fillna("").astype(str).str.strip()
    out["category_clean"] = category_clean.str.lower().str.title().mask(category_clean.eq(""), pd.NA)
    out["category_missing_flag"] = category_clean.eq("")

    amount_raw = out["raw_amount"]
    amount_blank = _blank_mask(amount_raw)
    amount_num = _to_numeric(amount_raw)
    out["amount_non_numeric_flag"] = (~amount_blank) & amount_num.isna()
    out["amount_negative_flag"] = amount_num < 0
    out["amount_nullified_flag"] = out["amount_negative_flag"]
    out["amount_clean"] = amount_num.mask(out["amount_negative_flag"])
    return out
