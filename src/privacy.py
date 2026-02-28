from __future__ import annotations

import hashlib
from typing import Any

import numpy as np
import pandas as pd

from . import config, schema


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
    """Redact PII values in an arbitrary nested record for safe printing/logging."""
    redacted: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, dict):
            redacted[key] = redact_record(value)
        elif isinstance(value, list):
            safe_items = []
            for item in value:
                if isinstance(item, dict):
                    safe_items.append(redact_record(item))
                else:
                    safe_items.append(item)
            redacted[key] = safe_items
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
            preview[column] = preview[column].apply(lambda v: _mask_text(v, "[REDACTED_NAME]"))
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
    """Create a deterministic SHA-256 hash from salt and seed."""
    return hashlib.sha256(f"{salt}|{seed}".encode("utf-8")).hexdigest()


def assign_applicant_pseudo_id(
    df: pd.DataFrame, salt: str = config.HASH_SALT
) -> tuple[pd.Series, pd.Series]:
    """Generate deterministic applicant pseudonyms and record source strategy."""
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
            seed = (
                f"name_dob_zip:{str(full_name).strip().lower()}|"
                f"{str(dob).strip()}|{str(zip_code).strip()}"
            )
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
    age_band = pd.cut(age_years, bins=bins, labels=labels, right=False)
    return age_band.astype("string")


def build_analysis_dataset(curated_full_df: pd.DataFrame) -> pd.DataFrame:
    """Create one-row-per-canonical-application PII-safe analysis dataframe."""
    analysis = curated_full_df.loc[curated_full_df["is_canonical_for_analysis"]].copy()
    analysis = analysis.sort_values(["application_id", "application_row_id"]).reset_index(drop=True)

    pseudo_id, pseudo_source = assign_applicant_pseudo_id(analysis)
    analysis["applicant_pseudo_id"] = pseudo_id
    analysis["pseudo_id_source"] = pseudo_source
    analysis["pseudo_id_fallback_used_flag"] = ~analysis["pseudo_id_source"].eq("ssn")

    analysis["age_band"] = _build_age_band(analysis["clean_date_of_birth"])
    analysis["age_band_missing_flag"] = analysis["age_band"].isna()

    direct_pii_cols = {
        "raw_applicant_full_name",
        "raw_applicant_email",
        "raw_applicant_ssn",
        "raw_applicant_ip_address",
        "raw_applicant_date_of_birth",
        "clean_email",
        "clean_date_of_birth",
    }
    existing_drop = [col for col in direct_pii_cols if col in analysis.columns]
    analysis = analysis.drop(columns=existing_drop)

    # Keep analysis outputs minimal to avoid leakage from audit/remediation fields
    # and to preserve separation of concerns between modelling and data operations.
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
    existing_analysis_columns = [col for col in analysis_columns if col in analysis.columns]
    analysis = analysis[existing_analysis_columns].copy()

    # Enforce one canonical row per application_id for safe analysis output.
    analysis = analysis.drop_duplicates(subset=["application_id"], keep="first")
    return analysis.reset_index(drop=True)


def generate_pii_inventory(
    *,
    curated_full_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
    spending_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build PII inventory by field path and dataset presence:
    - present_in uses pipe-delimited values from {raw, curated, analysis}.
    """
    field_to_columns: dict[str, list[str]] = {}
    field_to_meta: dict[str, dict[str, str]] = {}
    for entry in schema.APPLICATION_SCHEMA + schema.SPENDING_SCHEMA:
        field_path = str(entry["field_path"])
        field_to_columns.setdefault(field_path, []).append(str(entry["column"]))
        classification = str(entry["classification"])
        if classification not in {"PII", "Quasi-PII", "Non-PII"}:
            classification = "Quasi-PII"
        field_to_meta[field_path] = {
            "classification": classification,
            "notes": str(entry.get("notes", "")),
        }

    derived_fields = {
        "applicant_pseudo_id": {
            "columns": ["applicant_pseudo_id"],
            "classification": "Quasi-PII",
            "notes": "Salted SHA-256 pseudonym used for analysis linkage.",
        },
        "pseudo_id_source": {
            "columns": ["pseudo_id_source"],
            "classification": "Non-PII",
            "notes": "Indicates whether SSN or fallback source was used.",
        },
        "age_band": {
            "columns": ["age_band"],
            "classification": "Non-PII",
            "notes": "Privacy-preserving derived age representation.",
        },
    }

    rows: list[dict[str, str]] = []
    curated_cols = set(curated_full_df.columns)
    analysis_cols = set(analysis_df.columns)
    spending_cols = set(spending_df.columns)

    for field_path, columns in field_to_columns.items():
        present = ["raw"]
        if any(col in curated_cols or col in spending_cols for col in columns):
            present.append("curated")
        if any(col in analysis_cols for col in columns):
            present.append("analysis")
        meta = field_to_meta[field_path]
        rows.append(
            {
                "field_path": field_path,
                "classification": meta["classification"],
                "notes/purpose": meta["notes"],
                "present_in": "|".join(present),
            }
        )

    for field_path, meta in derived_fields.items():
        present: list[str] = []
        if any(col in curated_cols for col in meta["columns"]):
            present.append("curated")
        if any(col in analysis_cols for col in meta["columns"]):
            present.append("analysis")
        rows.append(
            {
                "field_path": field_path,
                "classification": meta["classification"],
                "notes/purpose": meta["notes"],
                "present_in": "|".join(present),
            }
        )

    return pd.DataFrame(rows).sort_values("field_path").reset_index(drop=True)
