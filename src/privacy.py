from __future__ import annotations

import hashlib
from typing import Any

import numpy as np
import pandas as pd

from . import config


def _mask_text(value: Any, replacement: str = "[REDACTED]") -> Any:
    """Mask non-blank scalar text values with a fixed replacement token."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return value
    value_str = str(value).strip()
    if value_str == "":
        return value
    return replacement


def _mask_email(value: Any) -> Any:
    """Mask emails while keeping a minimal domain-level preview."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return value
    value_str = str(value).strip()
    if value_str == "":
        return value
    if "@" not in value_str:
        return "[REDACTED_EMAIL]"
    local, domain = value_str.split("@", 1)
    local_masked = local[:1] + "***" if local else "***"
    return f"{local_masked}@{domain}"


def _mask_ssn(value: Any) -> Any:
    """Mask SSN-like values except for the last four characters."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return value
    value_str = str(value).strip()
    if value_str == "":
        return value
    return "***-**-" + value_str[-4:]


def _mask_ip(value: Any) -> Any:
    """Mask IP address values with a fixed token."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return value
    value_str = str(value).strip()
    if value_str == "":
        return value
    return "[REDACTED_IP]"


def _mask_dob(value: Any) -> Any:
    """Mask date-of-birth values while preserving only the year prefix."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return value
    value_str = str(value).strip()
    if value_str == "":
        return value
    year = value_str[:4] if len(value_str) >= 4 else "XXXX"
    return f"{year}-**-**"


def _redact_by_key(key: str, value: Any) -> Any:
    """Apply key-based redaction rules to scalar values."""
    key_lower = key.lower()
    if "ssn" in key_lower:
        return _mask_ssn(value)
    if "email" in key_lower:
        return _mask_email(value)
    if "ip" in key_lower:
        return _mask_ip(value)
    if "full_name" in key_lower or key_lower.endswith("name"):
        return _mask_text(value, "[REDACTED_NAME]")
    if "date_of_birth" in key_lower:
        return _mask_dob(value)
    return value


def redact_record(record: dict[str, Any]) -> dict[str, Any]:
    """Redact PII values in a nested record for safe printing and logging."""
    redacted: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, dict):
            redacted[key] = redact_record(value)
        elif isinstance(value, list):
            redacted[key] = [redact_record(item) if isinstance(item, dict) else item for item in value]
        else:
            redacted[key] = _redact_by_key(key, value)
    return redacted


def safe_preview_df(df: pd.DataFrame, pii_columns: list[str], n: int = 5) -> pd.DataFrame:
    """Return a redacted dataframe preview without exposing direct identifiers."""
    preview = df.head(n).copy()
    for column in pii_columns:
        if column not in preview.columns:
            continue
        if "ssn" in column:
            preview[column] = preview[column].apply(_mask_ssn)
        elif "email" in column:
            preview[column] = preview[column].apply(_mask_email)
        elif "ip" in column:
            preview[column] = preview[column].apply(_mask_ip)
        elif "date_of_birth" in column:
            preview[column] = preview[column].apply(_mask_dob)
        elif "full_name" in column:
            preview[column] = preview[column].apply(lambda value: _mask_text(value, "[REDACTED_NAME]"))
    return preview


def _is_blank(value: Any) -> bool:
    """Return True for null or empty-string values."""
    if value is None:
        return True
    if isinstance(value, float) and np.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _stable_hash(seed: str, salt: str) -> str:
    """Create a deterministic SHA-256 hash from a salt and seed."""
    return hashlib.sha256(f"{salt}|{seed}".encode("utf-8")).hexdigest()


def assign_applicant_pseudo_id(df: pd.DataFrame, salt: str = config.HASH_SALT) -> tuple[pd.Series, pd.Series]:
    """Generate deterministic applicant pseudonyms and capture the source strategy used."""
    pseudo_ids: list[str] = []
    pseudo_sources: list[str] = []
    for _, row in df.iterrows():
        ssn = row.get("raw_applicant_ssn")
        email = row.get("raw_applicant_email")
        full_name = row.get("raw_applicant_full_name")
        dob = row.get("raw_applicant_date_of_birth")
        zip_code = row.get("raw_applicant_zip_code")
        application_id = row.get("application_id")
        application_row_id = row.get("application_row_id")

        if not _is_blank(ssn):
            seed = f"ssn:{str(ssn).strip()}"
            source = "ssn"
        elif not _is_blank(email):
            seed = f"email:{str(email).strip().lower()}"
            source = "email_fallback"
        elif not (_is_blank(full_name) and _is_blank(dob) and _is_blank(zip_code)):
            seed = f"name_dob_zip:{str(full_name).strip().lower()}|{str(dob).strip()}|{str(zip_code).strip()}"
            source = "name_dob_zip_fallback"
        else:
            seed = f"application:{application_id}|row:{application_row_id}"
            source = "application_id_fallback"

        pseudo_ids.append(_stable_hash(seed, salt=salt))
        pseudo_sources.append(source)

    return pd.Series(pseudo_ids, index=df.index), pd.Series(pseudo_sources, index=df.index)


def _build_age_band(clean_dob: pd.Series) -> pd.Series:
    """Convert cleaned DOB values into coarse age bands for privacy-preserving analysis."""
    dob = pd.to_datetime(clean_dob, errors="coerce")
    reference = pd.Timestamp(config.ANALYSIS_REFERENCE_DATE)
    age_years = (reference - dob).dt.days / 365.25
    bins = [0, 25, 35, 45, 55, 65, np.inf]
    labels = ["<25", "25-34", "35-44", "45-54", "55-64", "65+"]
    return pd.cut(age_years, bins=bins, labels=labels, right=False).astype("string")


def build_analysis_dataset(curated_full_df: pd.DataFrame) -> pd.DataFrame:
    """Create a one-row-per-canonical-application PII-safe analysis dataset."""
    analysis = curated_full_df.loc[curated_full_df["is_canonical_for_analysis"]].copy()
    analysis = analysis.sort_values(["application_id", "application_row_id"]).reset_index(drop=True)

    pseudo_id, pseudo_source = assign_applicant_pseudo_id(analysis)
    analysis["applicant_pseudo_id"] = pseudo_id
    analysis["pseudo_id_source"] = pseudo_source
    analysis["pseudo_id_fallback_used_flag"] = ~analysis["pseudo_id_source"].eq("ssn")

    analysis["age_band"] = _build_age_band(analysis["clean_date_of_birth"])
    analysis["age_band_missing_flag"] = analysis["age_band"].isna()

    analysis = analysis.drop(columns=[column for column in config.DIRECT_PII_COLUMNS if column in analysis.columns])

    # Keep the analysis output narrow so audit or remediation fields do not leak
    # into downstream modelling logic or inflate the analytical surface area.
    analysis_columns = [
        "application_id",
        "applicant_pseudo_id",
        "pseudo_id_source",
        "pseudo_id_fallback_used_flag",
        "age_band",
        "age_band_missing_flag",
        "clean_gender",
        "clean_zip_code",
        "clean_annual_income",
        "clean_credit_history_months",
        "clean_debt_to_income",
        "clean_savings_balance",
        "clean_loan_approved",
        "clean_interest_rate",
        "clean_approved_amount",
        "clean_rejection_reason",
    ]
    analysis = analysis[[column for column in analysis_columns if column in analysis.columns]].copy()
    analysis = analysis.drop_duplicates(subset=["application_id"], keep="first")
    return analysis.reset_index(drop=True)


def generate_pii_inventory(*, curated_full_df: pd.DataFrame, analysis_df: pd.DataFrame) -> pd.DataFrame:
    """Build a minimal PII inventory for raw, curated, and analysis datasets."""
    curated_columns = set(curated_full_df.columns)
    analysis_columns = set(analysis_df.columns)
    rows = [
        {"field_path": "application_id", "classification": "Quasi-PII", "present_in_raw": True, "present_in_curated": "application_id" in curated_columns, "present_in_analysis": "application_id" in analysis_columns},
        {"field_path": "applicant_info.full_name", "classification": "PII", "present_in_raw": True, "present_in_curated": "raw_applicant_full_name" in curated_columns, "present_in_analysis": False},
        {"field_path": "applicant_info.email", "classification": "PII", "present_in_raw": True, "present_in_curated": "raw_applicant_email" in curated_columns, "present_in_analysis": False},
        {"field_path": "applicant_info.ssn", "classification": "PII", "present_in_raw": True, "present_in_curated": "raw_applicant_ssn" in curated_columns, "present_in_analysis": False},
        {"field_path": "applicant_info.ip_address", "classification": "PII", "present_in_raw": True, "present_in_curated": "raw_applicant_ip_address" in curated_columns, "present_in_analysis": False},
        {"field_path": "applicant_info.date_of_birth", "classification": "PII", "present_in_raw": True, "present_in_curated": "raw_applicant_date_of_birth" in curated_columns, "present_in_analysis": False},
        {"field_path": "applicant_info.gender", "classification": "Quasi-PII", "present_in_raw": True, "present_in_curated": "clean_gender" in curated_columns, "present_in_analysis": "clean_gender" in analysis_columns},
        {"field_path": "applicant_info.zip_code", "classification": "Quasi-PII", "present_in_raw": True, "present_in_curated": "clean_zip_code" in curated_columns, "present_in_analysis": "clean_zip_code" in analysis_columns},
        {"field_path": "applicant_pseudo_id", "classification": "Quasi-PII", "present_in_raw": False, "present_in_curated": False, "present_in_analysis": "applicant_pseudo_id" in analysis_columns},
        {"field_path": "age_band", "classification": "Non-PII", "present_in_raw": False, "present_in_curated": False, "present_in_analysis": "age_band" in analysis_columns},
    ]
    return pd.DataFrame(rows).sort_values("field_path").reset_index(drop=True)
