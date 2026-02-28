from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Any

import pandas as pd

from . import config


@dataclass(frozen=True)
class RuleDef:
    """Describe one quality rule and its stage-specific interpretation."""

    rule_id: str
    issue_group: str
    field_path: str
    description: str
    severity: str
    value_source_pre: str
    value_source_post: str
    field_path_annotated_pre: str | None = None
    field_path_annotated_post: str | None = None


APPLICATION_RULES: dict[str, RuleDef] = {
    "flag_missing_processing_timestamp": RuleDef("R_APP_001", "Completeness", "processing_timestamp", "Missing or blank processing timestamp.", "high", "raw", "clean"),
    "flag_missing_required_applicant_field": RuleDef("R_APP_002", "Completeness", "applicant_info.*", "One or more required applicant fields missing or blank.", "high", "raw", "clean"),
    "flag_missing_ssn_and_ip": RuleDef("R_APP_003", "Completeness", "applicant_info.ssn|applicant_info.ip_address", "Both SSN and IP address are missing or blank.", "high", "raw", "raw"),
    "flag_blank_email": RuleDef("R_APP_004", "Completeness", "applicant_info.email", "Email is missing or blank.", "medium", "raw", "clean"),
    "flag_invalid_email": RuleDef("R_APP_005", "Validity", "applicant_info.email", "Email does not match the expected format.", "medium", "raw", "clean"),
    "flag_gender_needs_normalisation": RuleDef("R_APP_006", "Consistency", "applicant_info.gender", "Gender is not already in canonical form.", "low", "raw", "clean", "applicant_info.gender_raw", "applicant_info.gender_clean"),
    "flag_invalid_gender": RuleDef("R_APP_007", "Validity", "applicant_info.gender", "Gender is outside the allowed values.", "medium", "raw", "clean"),
    "flag_dob_non_iso_format": RuleDef("R_APP_008", "Consistency", "applicant_info.date_of_birth", "Date of birth is not in canonical YYYY-MM-DD form.", "low", "raw", "clean", "applicant_info.date_of_birth_raw", "applicant_info.date_of_birth_clean"),
    "flag_dob_ambiguous_format": RuleDef("R_APP_009", "Consistency", "applicant_info.date_of_birth", f"Date of birth matches the ambiguous NN/NN/YYYY pattern. {config.DOB_AMBIGUITY_RULE}", "medium", "raw", "derived", "applicant_info.date_of_birth_raw", "applicant_info.date_of_birth_raw"),
    "flag_annual_income_string_type": RuleDef("R_APP_010", "Consistency", "financials.annual_income", "Annual income is stored as a string or cannot be coerced cleanly.", "low", "raw", "derived", "financials.annual_income_raw", "financials.annual_income_clean"),
    "flag_financial_field_drift_salary": RuleDef("R_APP_011", "Consistency", "financials.annual_salary", "Annual salary is populated instead of annual income.", "medium", "raw", "derived", "financials.annual_salary_raw", "financials.annual_income_clean"),
    "flag_credit_history_negative": RuleDef("R_APP_012", "Validity", "financials.credit_history_months", "Credit history months is negative.", "high", "raw", "clean"),
    "flag_savings_negative": RuleDef("R_APP_013", "Validity", "financials.savings_balance", "Savings balance is negative.", "high", "raw", "clean"),
    "flag_dti_out_of_range": RuleDef("R_APP_014", "Validity", "financials.debt_to_income", "Debt-to-income is outside the allowed range [0, 1].", "high", "raw", "clean"),
    "flag_approved_missing_required_fields": RuleDef("R_APP_015", "Cross-field logic", "decision.loan_approved", "Approved loan is missing interest_rate and/or approved_amount.", "high", "raw", "clean"),
    "flag_rejected_missing_reason": RuleDef("R_APP_016", "Cross-field logic", "decision.rejection_reason", "Rejected loan is missing rejection_reason.", "medium", "raw", "clean"),
    "flag_approved_credit_history_zero": RuleDef("R_APP_017", "Cross-field logic", "financials.credit_history_months", "Loan approved with zero months of credit history.", "medium", "raw", "clean"),
    "flag_approved_credit_history_lt6": RuleDef("R_APP_018", "Cross-field logic", "financials.credit_history_months", "Loan approved with less than 6 months of credit history.", "medium", "raw", "clean"),
    "flag_private_ip_address": RuleDef("R_APP_019", "Privacy", "applicant_info.ip_address", "IP address is in a private range and likely masked or synthetic.", "low", "raw", "raw"),
}

SPENDING_RULES: dict[str, RuleDef] = {
    "flag_spending_missing_category": RuleDef("R_SPN_001", "Completeness", "spending_behavior[].category", "Spending category is missing or blank.", "medium", "raw", "clean"),
    "flag_spending_amount_non_numeric": RuleDef("R_SPN_002", "Validity", "spending_behavior[].amount", "Spending amount cannot be parsed as numeric.", "high", "raw", "derived", "spending_behavior[].amount_raw", "spending_behavior[].amount_clean"),
    "flag_spending_amount_negative": RuleDef("R_SPN_003", "Validity", "spending_behavior[].amount", "Spending amount is negative.", "high", "raw", "clean", "spending_behavior[].amount_raw", "spending_behavior[].amount_clean"),
}

DUPLICATE_RULES: list[dict[str, str]] = [
    {"rule_id": "R_DUP_001", "issue_group": "Uniqueness", "field_path": "_id", "description": "Rows with duplicated application_id values.", "severity": "high", "value_source": "metadata"},
    {"rule_id": "R_DUP_002", "issue_group": "Uniqueness", "field_path": "_id", "description": "Distinct application_id values that are duplicated.", "severity": "high", "value_source": "metadata"},
    {"rule_id": "R_DUP_003", "issue_group": "Uniqueness", "field_path": "applicant_info.ssn", "description": "Rows where SSN repeats across one or more records.", "severity": "high", "value_source": "raw"},
    {"rule_id": "R_DUP_004", "issue_group": "Uniqueness", "field_path": "applicant_info.ssn", "description": "Distinct SSN values that appear across different application IDs.", "severity": "high", "value_source": "raw"},
    {"rule_id": "R_DUP_CONFLICT", "issue_group": "Uniqueness", "field_path": "_id", "description": "Duplicated application IDs classified as conflicts.", "severity": "high", "value_source": "metadata"},
    {"rule_id": "R_DUP_CANONICAL", "issue_group": "Remediation", "field_path": "_id", "description": "Canonical application rows retained for downstream analysis.", "severity": "medium", "value_source": "metadata"},
]

RULE_CATALOG_COLUMNS = [
    "stage",
    "rule_id",
    "issue_group",
    "field_path",
    "field_path_annotated",
    "value_source",
    "severity",
    "description",
]


def _blank_mask(series: pd.Series) -> pd.Series:
    """Return True where a Series contains null or blank string values."""
    as_obj = series.astype("object")
    return as_obj.isna() | as_obj.astype(str).str.strip().eq("")


def _to_bool(series: pd.Series) -> pd.Series:
    """Coerce a Series of mixed values into a nullable boolean-like object Series."""
    mapping = {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False, "y": True, "n": False}
    values: list[Any] = []
    for value in series:
        if isinstance(value, bool):
            values.append(value)
        elif pd.isna(value):
            values.append(pd.NA)
        else:
            values.append(mapping.get(str(value).strip().lower(), pd.NA))
    return pd.Series(values, index=series.index, dtype="object")


def _dob_ambiguous(series: pd.Series) -> pd.Series:
    """Return True for ambiguous NN/NN/YYYY DOB strings."""
    text = series.fillna("").astype(str).str.strip()
    pattern = text.str.match(r"^\d{2}/\d{2}/\d{4}$")
    parts = text.str.split("/", expand=True)
    if parts.shape[1] != 3:
        return pd.Series(False, index=series.index)
    left = pd.to_numeric(parts[0], errors="coerce")
    right = pd.to_numeric(parts[1], errors="coerce")
    return pattern & left.le(12) & right.le(12)


def _dob_non_iso(series: pd.Series) -> pd.Series:
    """Return True for non-blank DOB strings that are not ISO formatted."""
    text = series.fillna("").astype(str).str.strip()
    return text.ne("") & ~text.str.match(r"^\d{4}-\d{2}-\d{2}$")


def _private_ip(series: pd.Series) -> pd.Series:
    """Return True for IP addresses that fall into private ranges."""
    def is_private(value: Any) -> bool:
        if pd.isna(value):
            return False
        value_str = str(value).strip()
        if value_str == "":
            return False
        try:
            return ipaddress.ip_address(value_str).is_private
        except ValueError:
            return False
    return series.apply(is_private)


def _non_numeric_string(series: pd.Series) -> pd.Series:
    """Return True where non-blank values cannot be coerced to numeric."""
    text = series.fillna("").astype(str).str.strip()
    numeric = pd.to_numeric(series, errors="coerce")
    return text.ne("") & numeric.isna()


def build_rule_catalog() -> pd.DataFrame:
    """Build the minimal stage-aware rule catalog used to interpret reports."""
    rows: list[dict[str, Any]] = []
    for stage in ("pre", "post"):
        for rule in APPLICATION_RULES.values():
            rows.append({
                "stage": stage,
                "rule_id": rule.rule_id,
                "issue_group": rule.issue_group,
                "field_path": rule.field_path,
                "field_path_annotated": rule.field_path_annotated_pre if stage == "pre" else rule.field_path_annotated_post,
                "value_source": rule.value_source_pre if stage == "pre" else rule.value_source_post,
                "severity": rule.severity,
                "description": rule.description,
            })
        for rule in SPENDING_RULES.values():
            rows.append({
                "stage": stage,
                "rule_id": rule.rule_id,
                "issue_group": rule.issue_group,
                "field_path": rule.field_path,
                "field_path_annotated": rule.field_path_annotated_pre if stage == "pre" else rule.field_path_annotated_post,
                "value_source": rule.value_source_pre if stage == "pre" else rule.value_source_post,
                "severity": rule.severity,
                "description": rule.description,
            })
        for item in DUPLICATE_RULES:
            if item["rule_id"] in {"R_DUP_CONFLICT", "R_DUP_CANONICAL"} and stage == "pre":
                continue
            rows.append({
                "stage": stage,
                "rule_id": item["rule_id"],
                "issue_group": item["issue_group"],
                "field_path": item["field_path"],
                "field_path_annotated": pd.NA,
                "value_source": item["value_source"],
                "severity": item["severity"],
                "description": item["description"],
            })
    return pd.DataFrame(rows, columns=RULE_CATALOG_COLUMNS).drop_duplicates().reset_index(drop=True)


def validate_applications_preclean(df: pd.DataFrame) -> pd.DataFrame:
    """Evaluate application rules against raw flattened application columns."""
    flags = pd.DataFrame(index=df.index)
    flags["flag_missing_processing_timestamp"] = _blank_mask(df["raw_processing_timestamp"])
    flags["flag_missing_required_applicant_field"] = pd.concat([_blank_mask(df[column]) for column in config.REQUIRED_APPLICANT_RAW_COLUMNS], axis=1).any(axis=1)
    flags["flag_missing_ssn_and_ip"] = _blank_mask(df["raw_applicant_ssn"]) & _blank_mask(df["raw_applicant_ip_address"])
    flags["flag_blank_email"] = _blank_mask(df["raw_applicant_email"])

    email_text = df["raw_applicant_email"].fillna("").astype(str).str.strip()
    flags["flag_invalid_email"] = email_text.ne("") & ~email_text.str.match(config.EMAIL_REGEX)

    gender_text = df["raw_applicant_gender"].fillna("").astype(str).str.strip().str.lower()
    flags["flag_gender_needs_normalisation"] = gender_text.isin({"m", "f"})
    flags["flag_invalid_gender"] = gender_text.ne("") & ~gender_text.isin(set(config.GENDER_MAP))

    flags["flag_dob_non_iso_format"] = _dob_non_iso(df["raw_applicant_date_of_birth"])
    flags["flag_dob_ambiguous_format"] = _dob_ambiguous(df["raw_applicant_date_of_birth"])
    flags["flag_annual_income_string_type"] = _non_numeric_string(df["raw_financial_annual_income"])
    flags["flag_financial_field_drift_salary"] = _blank_mask(df["raw_financial_annual_income"]) & ~_blank_mask(df["raw_financial_annual_salary"])

    credit_history = pd.to_numeric(df["raw_financial_credit_history_months"], errors="coerce")
    savings = pd.to_numeric(df["raw_financial_savings_balance"], errors="coerce")
    dti = pd.to_numeric(df["raw_financial_debt_to_income"], errors="coerce")
    approved = _to_bool(df["raw_decision_loan_approved"])

    flags["flag_credit_history_negative"] = credit_history < 0
    flags["flag_savings_negative"] = savings < 0
    flags["flag_dti_out_of_range"] = (dti < 0) | (dti > 1)
    flags["flag_approved_missing_required_fields"] = approved.eq(True) & (pd.to_numeric(df["raw_decision_interest_rate"], errors="coerce").isna() | pd.to_numeric(df["raw_decision_approved_amount"], errors="coerce").isna())
    flags["flag_rejected_missing_reason"] = approved.eq(False) & _blank_mask(df["raw_decision_rejection_reason"])
    flags["flag_approved_credit_history_zero"] = approved.eq(True) & credit_history.eq(0)
    flags["flag_approved_credit_history_lt6"] = approved.eq(True) & credit_history.lt(6)
    flags["flag_private_ip_address"] = _private_ip(df["raw_applicant_ip_address"])
    return flags.fillna(False)


def validate_spending_preclean(df: pd.DataFrame) -> pd.DataFrame:
    """Evaluate spending rules against raw spending columns."""
    flags = pd.DataFrame(index=df.index)
    flags["flag_spending_missing_category"] = _blank_mask(df["raw_category"])
    flags["flag_spending_amount_non_numeric"] = _non_numeric_string(df["raw_amount"])
    amount = pd.to_numeric(df["raw_amount"], errors="coerce")
    flags["flag_spending_amount_negative"] = amount < 0
    return flags.fillna(False)


def validate_applications_postclean(df: pd.DataFrame) -> pd.DataFrame:
    """Evaluate application rules against cleaned application columns."""
    flags = pd.DataFrame(index=df.index)
    flags["flag_missing_processing_timestamp"] = _blank_mask(df["clean_processing_timestamp"])
    flags["flag_missing_required_applicant_field"] = pd.concat([
        _blank_mask(df["raw_applicant_full_name"]),
        _blank_mask(df["clean_email"]),
        _blank_mask(df["raw_applicant_ssn"]),
        _blank_mask(df["raw_applicant_ip_address"]),
        _blank_mask(df["clean_gender"]),
        _blank_mask(df["clean_date_of_birth"]),
        _blank_mask(df["clean_zip_code"]),
    ], axis=1).any(axis=1)
    flags["flag_missing_ssn_and_ip"] = _blank_mask(df["raw_applicant_ssn"]) & _blank_mask(df["raw_applicant_ip_address"])
    flags["flag_blank_email"] = _blank_mask(df["clean_email"])

    clean_email = df["clean_email"].fillna("").astype(str).str.strip()
    flags["flag_invalid_email"] = clean_email.ne("") & ~clean_email.str.match(config.EMAIL_REGEX)

    clean_gender = df["clean_gender"].fillna("").astype(str).str.strip()
    flags["flag_gender_needs_normalisation"] = clean_gender.ne("") & ~clean_gender.isin(["Male", "Female"])
    flags["flag_invalid_gender"] = clean_gender.ne("") & ~clean_gender.isin(["Male", "Female"])

    flags["flag_dob_non_iso_format"] = _dob_non_iso(df["clean_date_of_birth"])
    flags["flag_dob_ambiguous_format"] = df["dob_ambiguous_flag"].fillna(False).astype(bool)
    flags["flag_annual_income_string_type"] = df["clean_annual_income"].isna() & ~(_blank_mask(df["raw_financial_annual_income"]) & _blank_mask(df["raw_financial_annual_salary"]))
    flags["flag_financial_field_drift_salary"] = df["annual_income_from_salary_flag"].fillna(False).astype(bool)

    credit_history = pd.to_numeric(df["clean_credit_history_months"], errors="coerce")
    savings = pd.to_numeric(df["clean_savings_balance"], errors="coerce")
    dti = pd.to_numeric(df["clean_debt_to_income"], errors="coerce")
    approved = _to_bool(df["clean_loan_approved"])

    flags["flag_credit_history_negative"] = credit_history < 0
    flags["flag_savings_negative"] = savings < 0
    flags["flag_dti_out_of_range"] = (dti < 0) | (dti > 1)
    flags["flag_approved_missing_required_fields"] = df["approved_missing_terms_flag"].fillna(False).astype(bool)
    flags["flag_rejected_missing_reason"] = df["rejected_missing_reason_flag"].fillna(False).astype(bool)
    flags["flag_approved_credit_history_zero"] = approved.eq(True) & credit_history.eq(0)
    flags["flag_approved_credit_history_lt6"] = approved.eq(True) & credit_history.lt(6)
    flags["flag_private_ip_address"] = _private_ip(df["raw_applicant_ip_address"])
    return flags.fillna(False)


def validate_spending_postclean(df: pd.DataFrame) -> pd.DataFrame:
    """Evaluate spending rules against cleaned spending columns."""
    flags = pd.DataFrame(index=df.index)
    flags["flag_spending_missing_category"] = df["category_missing_flag"].fillna(False).astype(bool)
    flags["flag_spending_amount_non_numeric"] = df["amount_invalid_flag"].fillna(False).astype(bool)
    flags["flag_spending_amount_negative"] = df["amount_negative_flag"].fillna(False).astype(bool)
    return flags.fillna(False)
