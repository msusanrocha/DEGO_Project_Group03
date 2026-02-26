from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from . import config


@dataclass(frozen=True)
class RuleDef:
    rule_id: str
    issue_type: str
    field_path: str
    description: str
    severity: str


APPLICATION_SCHEMA: list[dict[str, Any]] = [
    {
        "field_path": "_id",
        "column": "application_id",
        "expected_dtype": "string",
        "required": True,
        "classification": "Quasi-PII",
        "notes": "External application identifier.",
    },
    {
        "field_path": "processing_timestamp",
        "column": "raw_processing_timestamp",
        "expected_dtype": "datetime-like string",
        "required": True,
        "classification": "Non-PII",
        "notes": "Processing event time used for canonical selection.",
    },
    {
        "field_path": "applicant_info.full_name",
        "column": "raw_applicant_full_name",
        "expected_dtype": "string",
        "required": True,
        "classification": "PII",
        "notes": "Direct identifier.",
    },
    {
        "field_path": "applicant_info.email",
        "column": "raw_applicant_email",
        "expected_dtype": "string",
        "required": True,
        "classification": "PII",
        "notes": "Direct identifier and contact field.",
    },
    {
        "field_path": "applicant_info.ssn",
        "column": "raw_applicant_ssn",
        "expected_dtype": "string",
        "required": True,
        "classification": "PII",
        "notes": "National identifier.",
    },
    {
        "field_path": "applicant_info.ip_address",
        "column": "raw_applicant_ip_address",
        "expected_dtype": "IPv4/IPv6 string",
        "required": True,
        "classification": "PII",
        "notes": "Device/network identifier.",
    },
    {
        "field_path": "applicant_info.gender",
        "column": "raw_applicant_gender",
        "expected_dtype": "string",
        "required": True,
        "classification": "Quasi-PII",
        "notes": "Allowed source variants: Male/Female/M/F.",
    },
    {
        "field_path": "applicant_info.date_of_birth",
        "column": "raw_applicant_date_of_birth",
        "expected_dtype": "date string",
        "required": True,
        "classification": "PII",
        "notes": "Source supports multiple date formats.",
    },
    {
        "field_path": "applicant_info.zip_code",
        "column": "raw_applicant_zip_code",
        "expected_dtype": "string",
        "required": True,
        "classification": "Quasi-PII",
        "notes": "Location quasi-identifier.",
    },
    {
        "field_path": "financials.annual_income",
        "column": "raw_financial_annual_income",
        "expected_dtype": "numeric",
        "required": False,
        "classification": "Non-PII",
        "notes": "May drift into annual_salary.",
    },
    {
        "field_path": "financials.annual_salary",
        "column": "raw_financial_annual_salary",
        "expected_dtype": "numeric",
        "required": False,
        "classification": "Non-PII",
        "notes": "Field drift variant for annual_income.",
    },
    {
        "field_path": "financials.credit_history_months",
        "column": "raw_financial_credit_history_months",
        "expected_dtype": "integer",
        "required": True,
        "classification": "Non-PII",
        "notes": "Must be >= 0.",
    },
    {
        "field_path": "financials.debt_to_income",
        "column": "raw_financial_debt_to_income",
        "expected_dtype": "numeric",
        "required": True,
        "classification": "Non-PII",
        "notes": "Expected range [0, 1].",
    },
    {
        "field_path": "financials.savings_balance",
        "column": "raw_financial_savings_balance",
        "expected_dtype": "numeric",
        "required": True,
        "classification": "Non-PII",
        "notes": "Must be >= 0.",
    },
    {
        "field_path": "decision.loan_approved",
        "column": "raw_decision_loan_approved",
        "expected_dtype": "boolean",
        "required": True,
        "classification": "Non-PII",
        "notes": "Decision flag.",
    },
    {
        "field_path": "decision.interest_rate",
        "column": "raw_decision_interest_rate",
        "expected_dtype": "numeric",
        "required": False,
        "classification": "Non-PII",
        "notes": "Required when loan_approved=True.",
    },
    {
        "field_path": "decision.approved_amount",
        "column": "raw_decision_approved_amount",
        "expected_dtype": "numeric",
        "required": False,
        "classification": "Non-PII",
        "notes": "Required when loan_approved=True.",
    },
    {
        "field_path": "decision.rejection_reason",
        "column": "raw_decision_rejection_reason",
        "expected_dtype": "string",
        "required": False,
        "classification": "Non-PII",
        "notes": "Required when loan_approved=False.",
    },
    {
        "field_path": "loan_purpose",
        "column": "raw_loan_purpose",
        "expected_dtype": "string",
        "required": False,
        "classification": "Non-PII",
        "notes": "Optional top-level context field.",
    },
    {
        "field_path": "notes",
        "column": "raw_notes",
        "expected_dtype": "string",
        "required": False,
        "classification": "Non-PII",
        "notes": "Optional operational notes.",
    },
]

SPENDING_SCHEMA: list[dict[str, Any]] = [
    {
        "field_path": "spending_behavior[].category",
        "column": "raw_category",
        "expected_dtype": "string",
        "required": True,
        "classification": "Non-PII",
        "notes": "Spending category label.",
    },
    {
        "field_path": "spending_behavior[].amount",
        "column": "raw_amount",
        "expected_dtype": "numeric",
        "required": True,
        "classification": "Non-PII",
        "notes": "Spending amount must be >= 0.",
    },
]


APPLICATION_RULES: dict[str, RuleDef] = {
    "flag_missing_processing_timestamp": RuleDef(
        rule_id="R_APP_001",
        issue_type="Completeness",
        field_path="processing_timestamp",
        description="Missing or blank processing timestamp.",
        severity="high",
    ),
    "flag_missing_required_applicant_field": RuleDef(
        rule_id="R_APP_002",
        issue_type="Completeness",
        field_path="applicant_info.*",
        description="One or more required applicant fields missing or blank.",
        severity="high",
    ),
    "flag_missing_ssn_and_ip": RuleDef(
        rule_id="R_APP_003",
        issue_type="Completeness",
        field_path="applicant_info.ssn|applicant_info.ip_address",
        description="Both SSN and IP address missing/blank.",
        severity="high",
    ),
    "flag_blank_email": RuleDef(
        rule_id="R_APP_004",
        issue_type="Completeness",
        field_path="applicant_info.email",
        description="Email missing or blank.",
        severity="medium",
    ),
    "flag_invalid_email": RuleDef(
        rule_id="R_APP_005",
        issue_type="Validity",
        field_path="applicant_info.email",
        description="Email does not match expected format.",
        severity="medium",
    ),
    "flag_gender_needs_normalisation": RuleDef(
        rule_id="R_APP_006",
        issue_type="Consistency",
        field_path="applicant_info.gender",
        description="Gender value requires canonical mapping (M/F to Male/Female).",
        severity="low",
    ),
    "flag_invalid_gender": RuleDef(
        rule_id="R_APP_007",
        issue_type="Validity",
        field_path="applicant_info.gender",
        description="Gender outside allowed source set.",
        severity="medium",
    ),
    "flag_dob_non_iso_format": RuleDef(
        rule_id="R_APP_008",
        issue_type="Consistency",
        field_path="applicant_info.date_of_birth",
        description="DOB not in canonical YYYY-MM-DD format.",
        severity="low",
    ),
    "flag_dob_ambiguous_format": RuleDef(
        rule_id="R_APP_009",
        issue_type="Consistency",
        field_path="applicant_info.date_of_birth",
        description="DOB format is ambiguous NN/NN/YYYY.",
        severity="medium",
    ),
    "flag_annual_income_string_type": RuleDef(
        rule_id="R_APP_010",
        issue_type="Consistency",
        field_path="financials.annual_income",
        description="Annual income stored as string instead of numeric.",
        severity="low",
    ),
    "flag_financial_field_drift_salary": RuleDef(
        rule_id="R_APP_011",
        issue_type="Consistency",
        field_path="financials.annual_salary",
        description="Annual salary provided while annual income missing (field drift).",
        severity="medium",
    ),
    "flag_credit_history_negative": RuleDef(
        rule_id="R_APP_012",
        issue_type="Validity",
        field_path="financials.credit_history_months",
        description="Credit history months is negative.",
        severity="high",
    ),
    "flag_savings_negative": RuleDef(
        rule_id="R_APP_013",
        issue_type="Validity",
        field_path="financials.savings_balance",
        description="Savings balance is negative.",
        severity="high",
    ),
    "flag_dti_out_of_range": RuleDef(
        rule_id="R_APP_014",
        issue_type="Validity",
        field_path="financials.debt_to_income",
        description="Debt-to-income is outside [0, 1].",
        severity="high",
    ),
    "flag_approved_missing_required_fields": RuleDef(
        rule_id="R_APP_015",
        issue_type="Cross-field logic",
        field_path="decision.loan_approved",
        description="Approved loan missing approved_amount and/or interest_rate.",
        severity="high",
    ),
    "flag_rejected_missing_reason": RuleDef(
        rule_id="R_APP_016",
        issue_type="Cross-field logic",
        field_path="decision.rejection_reason",
        description="Rejected loan missing rejection_reason.",
        severity="medium",
    ),
    "flag_approved_credit_history_zero": RuleDef(
        rule_id="R_APP_017",
        issue_type="Plausibility",
        field_path="financials.credit_history_months",
        description="Loan approved with zero months of credit history.",
        severity="medium",
    ),
    "flag_approved_credit_history_lt6": RuleDef(
        rule_id="R_APP_018",
        issue_type="Plausibility",
        field_path="financials.credit_history_months",
        description="Loan approved with less than 6 months of credit history.",
        severity="medium",
    ),
    "flag_private_ip_address": RuleDef(
        rule_id="R_APP_019",
        issue_type="Synthetic indicator",
        field_path="applicant_info.ip_address",
        description="IP address is private-range (likely masked/synthetic).",
        severity="low",
    ),
}

SPENDING_RULES: dict[str, RuleDef] = {
    "flag_spending_missing_category": RuleDef(
        rule_id="R_SPN_001",
        issue_type="Completeness",
        field_path="spending_behavior[].category",
        description="Spending category missing or blank.",
        severity="medium",
    ),
    "flag_spending_amount_non_numeric": RuleDef(
        rule_id="R_SPN_002",
        issue_type="Validity",
        field_path="spending_behavior[].amount",
        description="Spending amount cannot be parsed as numeric.",
        severity="high",
    ),
    "flag_spending_amount_negative": RuleDef(
        rule_id="R_SPN_003",
        issue_type="Validity",
        field_path="spending_behavior[].amount",
        description="Spending amount is negative.",
        severity="high",
    ),
}

APPLICATION_RULES_POST: dict[str, RuleDef] = {
    key: RuleDef(
        rule_id=rule.rule_id,
        issue_type=rule.issue_type,
        field_path=rule.field_path,
        description=rule.description,
        severity=rule.severity,
    )
    for key, rule in APPLICATION_RULES.items()
}
APPLICATION_RULES_POST["flag_gender_needs_normalisation"] = RuleDef(
    rule_id="R_APP_006",
    issue_type="Consistency",
    field_path="applicant_info.gender",
    description="Gender remains non-canonical after cleaning.",
    severity="low",
)
APPLICATION_RULES_POST["flag_annual_income_string_type"] = RuleDef(
    rule_id="R_APP_010",
    issue_type="Consistency",
    field_path="financials.annual_income",
    description="Annual income could not be coerced to numeric after cleaning.",
    severity="low",
)
APPLICATION_RULES_POST["flag_dob_non_iso_format"] = RuleDef(
    rule_id="R_APP_008",
    issue_type="Consistency",
    field_path="applicant_info.date_of_birth",
    description="Clean DOB is non-ISO after parsing.",
    severity="low",
)

SPENDING_RULES_POST: dict[str, RuleDef] = {
    key: RuleDef(
        rule_id=rule.rule_id,
        issue_type=rule.issue_type,
        field_path=rule.field_path,
        description=rule.description,
        severity=rule.severity,
    )
    for key, rule in SPENDING_RULES.items()
}
SPENDING_RULES_POST["flag_spending_amount_negative"] = RuleDef(
    rule_id="R_SPN_003",
    issue_type="Validity",
    field_path="spending_behavior[].amount",
    description="Clean spending amount remains negative after remediation.",
    severity="high",
)

RULE_CATALOG_COLUMNS = [
    "stage",
    "rule_id",
    "rule_key",
    "rule_family",
    "issue_type",
    "field_path",
    "field_path_annotated",
    "source_columns",
    "value_source",
    "dataset_scope",
    "denominator",
    "count_unit",
    "severity",
    "description",
]


def build_rule_catalog() -> pd.DataFrame:
    """Build a consolidated stage-aware rule catalog for all pipeline reports."""
    app_pre_annotations = {
        "flag_missing_processing_timestamp": {
            "field_path_annotated": "processing_timestamp_raw",
            "source_columns": "raw_processing_timestamp",
            "value_source": "raw",
        },
        "flag_missing_required_applicant_field": {
            "field_path_annotated": "applicant_info.required_fields_raw",
            "source_columns": (
                "raw_applicant_full_name|raw_applicant_email|raw_applicant_ssn|"
                "raw_applicant_ip_address|raw_applicant_gender|"
                "raw_applicant_date_of_birth|raw_applicant_zip_code"
            ),
            "value_source": "raw",
        },
        "flag_missing_ssn_and_ip": {
            "field_path_annotated": "applicant_info.ssn_raw|applicant_info.ip_address_raw",
            "source_columns": "raw_applicant_ssn|raw_applicant_ip_address",
            "value_source": "raw",
        },
        "flag_blank_email": {
            "field_path_annotated": "applicant_info.email_raw",
            "source_columns": "raw_applicant_email",
            "value_source": "raw",
        },
        "flag_invalid_email": {
            "field_path_annotated": "applicant_info.email_raw",
            "source_columns": "raw_applicant_email",
            "value_source": "raw",
        },
        "flag_gender_needs_normalisation": {
            "field_path_annotated": "applicant_info.gender_raw",
            "source_columns": "raw_applicant_gender",
            "value_source": "raw",
        },
        "flag_invalid_gender": {
            "field_path_annotated": "applicant_info.gender_raw",
            "source_columns": "raw_applicant_gender",
            "value_source": "raw",
        },
        "flag_dob_non_iso_format": {
            "field_path_annotated": "applicant_info.date_of_birth_raw",
            "source_columns": "raw_applicant_date_of_birth",
            "value_source": "raw",
        },
        "flag_dob_ambiguous_format": {
            "field_path_annotated": "applicant_info.date_of_birth_raw",
            "source_columns": "raw_applicant_date_of_birth",
            "value_source": "raw",
        },
        "flag_annual_income_string_type": {
            "field_path_annotated": "financials.annual_income_raw",
            "source_columns": "raw_financial_annual_income",
            "value_source": "raw",
        },
        "flag_financial_field_drift_salary": {
            "field_path_annotated": "financials.annual_income_raw|financials.annual_salary_raw",
            "source_columns": "raw_financial_annual_income|raw_financial_annual_salary",
            "value_source": "raw",
        },
        "flag_credit_history_negative": {
            "field_path_annotated": "financials.credit_history_months_raw",
            "source_columns": "raw_financial_credit_history_months",
            "value_source": "raw",
        },
        "flag_savings_negative": {
            "field_path_annotated": "financials.savings_balance_raw",
            "source_columns": "raw_financial_savings_balance",
            "value_source": "raw",
        },
        "flag_dti_out_of_range": {
            "field_path_annotated": "financials.debt_to_income_raw",
            "source_columns": "raw_financial_debt_to_income",
            "value_source": "raw",
        },
        "flag_approved_missing_required_fields": {
            "field_path_annotated": "decision.loan_approved_raw",
            "source_columns": "raw_decision_loan_approved|raw_decision_approved_amount|raw_decision_interest_rate",
            "value_source": "raw",
        },
        "flag_rejected_missing_reason": {
            "field_path_annotated": "decision.loan_approved_raw|decision.rejection_reason_raw",
            "source_columns": "raw_decision_loan_approved|raw_decision_rejection_reason",
            "value_source": "raw",
        },
        "flag_approved_credit_history_zero": {
            "field_path_annotated": "decision.loan_approved_raw|financials.credit_history_months_raw",
            "source_columns": "raw_decision_loan_approved|raw_financial_credit_history_months",
            "value_source": "raw",
        },
        "flag_approved_credit_history_lt6": {
            "field_path_annotated": "decision.loan_approved_raw|financials.credit_history_months_raw",
            "source_columns": "raw_decision_loan_approved|raw_financial_credit_history_months",
            "value_source": "raw",
        },
        "flag_private_ip_address": {
            "field_path_annotated": "applicant_info.ip_address_raw",
            "source_columns": "raw_applicant_ip_address",
            "value_source": "raw",
        },
    }

    app_post_annotations = {
        "flag_missing_processing_timestamp": {
            "field_path_annotated": "processing_timestamp_clean",
            "source_columns": "clean_processing_timestamp",
            "value_source": "clean",
        },
        "flag_missing_required_applicant_field": {
            "field_path_annotated": "applicant_info.required_fields_mixed",
            "source_columns": (
                "raw_applicant_full_name|clean_email|raw_applicant_ssn|"
                "raw_applicant_ip_address|clean_gender|clean_date_of_birth|clean_zip_code"
            ),
            "value_source": "derived",
        },
        "flag_missing_ssn_and_ip": {
            "field_path_annotated": "applicant_info.ssn_raw|applicant_info.ip_address_raw",
            "source_columns": "raw_applicant_ssn|raw_applicant_ip_address",
            "value_source": "raw",
        },
        "flag_blank_email": {
            "field_path_annotated": "applicant_info.email_clean",
            "source_columns": "clean_email",
            "value_source": "clean",
        },
        "flag_invalid_email": {
            "field_path_annotated": "applicant_info.email_clean",
            "source_columns": "clean_email",
            "value_source": "clean",
        },
        "flag_gender_needs_normalisation": {
            "field_path_annotated": "applicant_info.gender_clean",
            "source_columns": "clean_gender",
            "value_source": "clean",
        },
        "flag_invalid_gender": {
            "field_path_annotated": "applicant_info.gender_clean",
            "source_columns": "clean_gender|gender_invalid_flag",
            "value_source": "derived",
        },
        "flag_dob_non_iso_format": {
            "field_path_annotated": "applicant_info.date_of_birth_clean",
            "source_columns": "clean_date_of_birth",
            "value_source": "clean",
        },
        "flag_dob_ambiguous_format": {
            "field_path_annotated": "applicant_info.date_of_birth_raw",
            "source_columns": "dob_ambiguous_flag",
            "value_source": "derived",
            "description_override": (
                "DOB ambiguity flag carried from cleaning stage. "
                f"Rule: {config.DOB_AMBIGUITY_RULE}"
            ),
        },
        "flag_annual_income_string_type": {
            "field_path_annotated": "financials.annual_income_clean",
            "source_columns": "annual_income_coerce_failed_flag",
            "value_source": "derived",
        },
        "flag_financial_field_drift_salary": {
            "field_path_annotated": "financials.annual_income_clean",
            "source_columns": "annual_income_from_salary_flag",
            "value_source": "derived",
        },
        "flag_credit_history_negative": {
            "field_path_annotated": "financials.credit_history_months_clean",
            "source_columns": "clean_credit_history_months",
            "value_source": "clean",
        },
        "flag_savings_negative": {
            "field_path_annotated": "financials.savings_balance_clean",
            "source_columns": "clean_savings_balance",
            "value_source": "clean",
        },
        "flag_dti_out_of_range": {
            "field_path_annotated": "financials.debt_to_income_clean",
            "source_columns": "clean_debt_to_income",
            "value_source": "clean",
        },
        "flag_approved_missing_required_fields": {
            "field_path_annotated": "decision.loan_approved_clean",
            "source_columns": "clean_loan_approved|clean_approved_amount|clean_interest_rate",
            "value_source": "clean",
        },
        "flag_rejected_missing_reason": {
            "field_path_annotated": "decision.loan_approved_clean|decision.rejection_reason_clean",
            "source_columns": "clean_loan_approved|clean_rejection_reason",
            "value_source": "clean",
        },
        "flag_approved_credit_history_zero": {
            "field_path_annotated": "decision.loan_approved_clean|financials.credit_history_months_clean",
            "source_columns": "clean_loan_approved|clean_credit_history_months",
            "value_source": "clean",
        },
        "flag_approved_credit_history_lt6": {
            "field_path_annotated": "decision.loan_approved_clean|financials.credit_history_months_clean",
            "source_columns": "clean_loan_approved|clean_credit_history_months",
            "value_source": "clean",
        },
        "flag_private_ip_address": {
            "field_path_annotated": "applicant_info.ip_address_raw",
            "source_columns": "raw_applicant_ip_address",
            "value_source": "raw",
        },
    }

    spending_pre_annotations = {
        "flag_spending_missing_category": {
            "field_path_annotated": "spending_behavior[].category_raw",
            "source_columns": "raw_category",
            "value_source": "raw",
        },
        "flag_spending_amount_non_numeric": {
            "field_path_annotated": "spending_behavior[].amount_raw",
            "source_columns": "raw_amount",
            "value_source": "raw",
        },
        "flag_spending_amount_negative": {
            "field_path_annotated": "spending_behavior[].amount_raw",
            "source_columns": "raw_amount",
            "value_source": "raw",
        },
    }
    spending_post_annotations = {
        "flag_spending_missing_category": {
            "field_path_annotated": "spending_behavior[].category_clean",
            "source_columns": "category_clean",
            "value_source": "clean",
        },
        "flag_spending_amount_non_numeric": {
            "field_path_annotated": "spending_behavior[].amount_clean",
            "source_columns": "amount_non_numeric_flag",
            "value_source": "derived",
        },
        "flag_spending_amount_negative": {
            "field_path_annotated": "spending_behavior[].amount_clean",
            "source_columns": "amount_clean",
            "value_source": "clean",
        },
    }

    rows: list[dict[str, Any]] = []
    for stage, rules, annotation_map in (
        ("pre", APPLICATION_RULES, app_pre_annotations),
        ("post", APPLICATION_RULES_POST, app_post_annotations),
    ):
        for rule_key, rule in rules.items():
            annotation = annotation_map.get(rule_key, {})
            rows.append(
                {
                    "stage": stage,
                    "rule_id": rule.rule_id,
                    "rule_key": rule_key,
                    "rule_family": "APP",
                    "issue_type": rule.issue_type,
                    "field_path": rule.field_path,
                    "field_path_annotated": annotation.get("field_path_annotated", rule.field_path),
                    "source_columns": annotation.get("source_columns", ""),
                    "value_source": annotation.get("value_source", "derived"),
                    "dataset_scope": "raw" if stage == "pre" else "curated",
                    "denominator": "application_rows",
                    "count_unit": "rows",
                    "severity": rule.severity,
                    "description": annotation.get("description_override", rule.description),
                }
            )

    for stage, rules, annotation_map in (
        ("pre", SPENDING_RULES, spending_pre_annotations),
        ("post", SPENDING_RULES_POST, spending_post_annotations),
    ):
        for rule_key, rule in rules.items():
            annotation = annotation_map.get(rule_key, {})
            rows.append(
                {
                    "stage": stage,
                    "rule_id": rule.rule_id,
                    "rule_key": rule_key,
                    "rule_family": "SPN",
                    "issue_type": rule.issue_type,
                    "field_path": rule.field_path,
                    "field_path_annotated": annotation.get("field_path_annotated", rule.field_path),
                    "source_columns": annotation.get("source_columns", ""),
                    "value_source": annotation.get("value_source", "derived"),
                    "dataset_scope": "raw" if stage == "pre" else "curated",
                    "denominator": "spending_rows",
                    "count_unit": "rows",
                    "severity": rule.severity,
                    "description": rule.description,
                }
            )

    duplicate_rules = [
        {
            "rule_id": "R_DUP_001",
            "rule_key": "dup_rows_application_id",
            "issue_type": "Uniqueness",
            "field_path": "_id",
            "field_path_annotated": "_id_duplicate_rows",
            "source_columns": "application_id|is_duplicate_id",
            "count_unit": "rows",
            "description": "Rows with duplicated application_id values.",
        },
        {
            "rule_id": "R_DUP_002",
            "rule_key": "dup_distinct_application_id",
            "issue_type": "Uniqueness",
            "field_path": "_id",
            "field_path_annotated": "_id_duplicate_distinct_ids",
            "source_columns": "application_id",
            "count_unit": "distinct_ids",
            "description": "Distinct application_id keys that are duplicated.",
        },
        {
            "rule_id": "R_DUP_003",
            "rule_key": "dup_rows_ssn",
            "issue_type": "Uniqueness",
            "field_path": "applicant_info.ssn",
            "field_path_annotated": "applicant_info.ssn_duplicate_rows",
            "source_columns": "raw_applicant_ssn|application_id",
            "count_unit": "rows",
            "description": "Rows where SSN repeats across one or more records.",
        },
        {
            "rule_id": "R_DUP_004",
            "rule_key": "dup_distinct_ssn_cross_app",
            "issue_type": "Uniqueness",
            "field_path": "applicant_info.ssn",
            "field_path_annotated": "applicant_info.ssn_cross_application_distinct",
            "source_columns": "raw_applicant_ssn|application_id",
            "count_unit": "distinct_ids",
            "description": "Distinct SSN values that appear across different application IDs.",
        },
    ]
    for stage in ("pre", "post"):
        for item in duplicate_rules:
            rows.append(
                {
                    "stage": stage,
                    "rule_id": item["rule_id"],
                    "rule_key": item["rule_key"],
                    "rule_family": "DUP",
                    "issue_type": item["issue_type"],
                    "field_path": item["field_path"],
                    "field_path_annotated": item["field_path_annotated"],
                    "source_columns": item["source_columns"],
                    "value_source": "metadata" if item["rule_id"] in {"R_DUP_001", "R_DUP_002"} else "raw",
                    "dataset_scope": "quality",
                    "denominator": "application_rows",
                    "count_unit": item["count_unit"],
                    "severity": "high",
                    "description": item["description"],
                }
            )

    kpi_rules = [
        {
            "rule_id": "R_DUP_CONFLICT",
            "rule_key": "kpi_duplicate_conflict_ids",
            "issue_type": "KPI",
            "field_path": "_id",
            "field_path_annotated": "_id_duplicate_conflict_distinct",
            "source_columns": "classification|application_id",
            "denominator": "application_rows",
            "count_unit": "distinct_ids",
            "severity": "medium",
            "description": "Distinct application_id values classified as duplicate conflicts.",
        },
        {
            "rule_id": "R_DUP_CANONICAL",
            "rule_key": "kpi_canonical_rows_selected",
            "issue_type": "KPI",
            "field_path": "_id",
            "field_path_annotated": "_id_canonical_analysis_rows",
            "source_columns": "is_canonical_for_analysis|application_row_id|application_id",
            "denominator": "application_rows",
            "count_unit": "rows",
            "severity": "low",
            "description": "Rows selected as canonical records for analysis output.",
        },
    ]
    for item in kpi_rules:
        rows.append(
            {
                "stage": "post",
                "rule_id": item["rule_id"],
                "rule_key": item["rule_key"],
                "rule_family": "KPI",
                "issue_type": item["issue_type"],
                "field_path": item["field_path"],
                "field_path_annotated": item["field_path_annotated"],
                "source_columns": item["source_columns"],
                "value_source": "metadata",
                "dataset_scope": "quality",
                "denominator": item["denominator"],
                "count_unit": item["count_unit"],
                "severity": item["severity"],
                "description": item["description"],
            }
        )

    catalog = pd.DataFrame(rows, columns=RULE_CATALOG_COLUMNS)
    catalog = catalog.sort_values(["stage", "rule_family", "rule_id"]).reset_index(drop=True)
    return catalog


def schema_dictionary_df() -> pd.DataFrame:
    """Return a tabular data dictionary for application and spending schemas."""
    app_df = pd.DataFrame(APPLICATION_SCHEMA)
    app_df.insert(0, "dataset", "applications")
    spending_df = pd.DataFrame(SPENDING_SCHEMA)
    spending_df.insert(0, "dataset", "spending_items")
    return pd.concat([app_df, spending_df], ignore_index=True)


def _blank_mask(series: pd.Series) -> pd.Series:
    """Return True for null/blank values in a pandas Series."""
    series_obj = series.astype("object")
    as_str = series_obj.astype(str).str.strip()
    return series_obj.isna() | as_str.eq("")


def _to_numeric(series: pd.Series) -> pd.Series:
    """Convert a Series to numeric values with invalid parsing as NaN."""
    return pd.to_numeric(series, errors="coerce")


def _to_bool(series: pd.Series) -> pd.Series:
    """Convert common string/number boolean encodings to a nullable Series."""
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
    out = []
    for value in series:
        if isinstance(value, bool):
            out.append(value)
            continue
        if value is None or (isinstance(value, float) and np.isnan(value)):
            out.append(np.nan)
            continue
        value_str = str(value).strip().lower()
        out.append(mapping.get(value_str, np.nan))
    return pd.Series(out, index=series.index, dtype="object")


def _column_or_na(df: pd.DataFrame, column: str) -> pd.Series:
    """Return a dataframe column when present, else an NA-filled fallback Series."""
    if column in df.columns:
        return df[column]
    return pd.Series([pd.NA] * len(df.index), index=df.index, dtype="object")


def _is_private_ip(value: Any) -> bool:
    """Check whether a candidate IP address string is in a private range."""
    if value is None:
        return False
    value_str = str(value).strip()
    if value_str == "":
        return False
    try:
        return ipaddress.ip_address(value_str).is_private
    except ValueError:
        return False


def validate_applications_preclean(df: pd.DataFrame) -> pd.DataFrame:
    """Compute pre-clean application-level validation flags from raw columns."""
    email_re = re.compile(config.EMAIL_REGEX)
    flags = pd.DataFrame(index=df.index)

    email_blank = _blank_mask(df["raw_applicant_email"])
    ssn_blank = _blank_mask(df["raw_applicant_ssn"])
    ip_blank = _blank_mask(df["raw_applicant_ip_address"])
    gender_blank = _blank_mask(df["raw_applicant_gender"])
    dob_blank = _blank_mask(df["raw_applicant_date_of_birth"])
    zip_blank = _blank_mask(df["raw_applicant_zip_code"])
    name_blank = _blank_mask(df["raw_applicant_full_name"])
    ts_blank = _blank_mask(df["raw_processing_timestamp"])

    flags["flag_missing_processing_timestamp"] = ts_blank
    flags["flag_missing_required_applicant_field"] = (
        name_blank | email_blank | ssn_blank | ip_blank | gender_blank | dob_blank | zip_blank
    )
    flags["flag_missing_ssn_and_ip"] = ssn_blank & ip_blank
    flags["flag_blank_email"] = email_blank

    email_clean = df["raw_applicant_email"].fillna("").astype(str).str.strip()
    flags["flag_invalid_email"] = (~email_blank) & (~email_clean.str.match(email_re))

    gender_clean = df["raw_applicant_gender"].fillna("").astype(str).str.strip().str.lower()
    flags["flag_gender_needs_normalisation"] = gender_clean.isin({"m", "f"})
    flags["flag_invalid_gender"] = (~gender_blank) & (~gender_clean.isin(set(config.GENDER_MAP.keys())))

    dob_str = df["raw_applicant_date_of_birth"].fillna("").astype(str).str.strip()
    flags["flag_dob_non_iso_format"] = (~dob_blank) & (~dob_str.str.match(r"^\d{4}-\d{2}-\d{2}$"))
    ambiguous_pattern = dob_str.str.match(r"^\d{2}/\d{2}/\d{4}$")
    left_num = pd.to_numeric(dob_str.str.slice(0, 2), errors="coerce")
    right_num = pd.to_numeric(dob_str.str.slice(3, 5), errors="coerce")
    flags["flag_dob_ambiguous_format"] = ambiguous_pattern & (left_num <= 12) & (right_num <= 12)

    income_raw = df["raw_financial_annual_income"]
    flags["flag_annual_income_string_type"] = income_raw.apply(
        lambda v: isinstance(v, str) and v.strip() != ""
    )
    income_blank = _blank_mask(df["raw_financial_annual_income"])
    salary_blank = _blank_mask(df["raw_financial_annual_salary"])
    flags["flag_financial_field_drift_salary"] = income_blank & (~salary_blank)

    credit_num = _to_numeric(df["raw_financial_credit_history_months"])
    dti_num = _to_numeric(df["raw_financial_debt_to_income"])
    savings_num = _to_numeric(df["raw_financial_savings_balance"])

    flags["flag_credit_history_negative"] = credit_num < 0
    flags["flag_savings_negative"] = savings_num < 0
    flags["flag_dti_out_of_range"] = (dti_num < 0) | (dti_num > 1)

    approved = _to_bool(df["raw_decision_loan_approved"]).eq(True)
    rejected = _to_bool(df["raw_decision_loan_approved"]).eq(False)
    approved_amount_blank = _blank_mask(df["raw_decision_approved_amount"])
    interest_blank = _blank_mask(df["raw_decision_interest_rate"])
    rejection_blank = _blank_mask(df["raw_decision_rejection_reason"])

    flags["flag_approved_missing_required_fields"] = approved & (
        approved_amount_blank | interest_blank
    )
    flags["flag_rejected_missing_reason"] = rejected & rejection_blank

    flags["flag_approved_credit_history_zero"] = approved & credit_num.eq(0)
    flags["flag_approved_credit_history_lt6"] = approved & (credit_num < 6)
    flags["flag_private_ip_address"] = df["raw_applicant_ip_address"].apply(_is_private_ip)

    return flags.fillna(False).astype(bool)


def validate_spending_preclean(df: pd.DataFrame) -> pd.DataFrame:
    """Compute pre-clean spending-level validation flags from raw columns."""
    flags = pd.DataFrame(index=df.index)
    category_blank = _blank_mask(df["raw_category"])
    amount_blank = _blank_mask(df["raw_amount"])
    amount_num = _to_numeric(df["raw_amount"])
    flags["flag_spending_missing_category"] = category_blank
    flags["flag_spending_amount_non_numeric"] = (~amount_blank) & amount_num.isna()
    flags["flag_spending_amount_negative"] = amount_num < 0
    return flags.fillna(False).astype(bool)


def validate_applications_postclean(df: pd.DataFrame) -> pd.DataFrame:
    """Compute post-clean application validation flags using cleaned columns."""
    email_re = re.compile(config.EMAIL_REGEX)
    flags = pd.DataFrame(index=df.index)

    clean_ts = _column_or_na(df, "clean_processing_timestamp")
    clean_email = _column_or_na(df, "clean_email")
    clean_gender = _column_or_na(df, "clean_gender")
    clean_dob = _column_or_na(df, "clean_date_of_birth")
    clean_zip = _column_or_na(df, "clean_zip_code")

    name_blank = _blank_mask(_column_or_na(df, "raw_applicant_full_name"))
    email_blank = _blank_mask(clean_email)
    ssn_blank = _blank_mask(_column_or_na(df, "raw_applicant_ssn"))
    ip_blank = _blank_mask(_column_or_na(df, "raw_applicant_ip_address"))
    gender_blank = _blank_mask(clean_gender)
    dob_blank = _blank_mask(clean_dob)
    zip_blank = _blank_mask(clean_zip)

    flags["flag_missing_processing_timestamp"] = _blank_mask(clean_ts)
    flags["flag_missing_required_applicant_field"] = (
        name_blank | email_blank | ssn_blank | ip_blank | gender_blank | dob_blank | zip_blank
    )
    flags["flag_missing_ssn_and_ip"] = ssn_blank & ip_blank
    flags["flag_blank_email"] = email_blank

    email_norm = clean_email.fillna("").astype(str).str.strip()
    flags["flag_invalid_email"] = (~email_blank) & (~email_norm.str.match(email_re))

    flags["flag_gender_needs_normalisation"] = pd.Series(False, index=df.index)
    clean_gender_norm = clean_gender.fillna("").astype(str).str.strip()
    gender_invalid_existing = _column_or_na(df, "gender_invalid_flag").fillna(False).astype(bool)
    flags["flag_invalid_gender"] = gender_invalid_existing | (
        (~gender_blank) & (~clean_gender_norm.isin({"Male", "Female"}))
    )

    clean_dob_norm = clean_dob.fillna("").astype(str).str.strip()
    flags["flag_dob_non_iso_format"] = (~dob_blank) & (~clean_dob_norm.str.match(r"^\d{4}-\d{2}-\d{2}$"))
    flags["flag_dob_ambiguous_format"] = _column_or_na(df, "dob_ambiguous_flag").fillna(False).astype(bool)

    flags["flag_annual_income_string_type"] = (
        _column_or_na(df, "annual_income_coerce_failed_flag").fillna(False).astype(bool)
    )
    flags["flag_financial_field_drift_salary"] = (
        _column_or_na(df, "annual_income_from_salary_flag").fillna(False).astype(bool)
    )

    clean_credit_history = _to_numeric(_column_or_na(df, "clean_credit_history_months"))
    clean_dti = _to_numeric(_column_or_na(df, "clean_debt_to_income"))
    clean_savings = _to_numeric(_column_or_na(df, "clean_savings_balance"))
    flags["flag_credit_history_negative"] = clean_credit_history < 0
    flags["flag_savings_negative"] = clean_savings < 0
    flags["flag_dti_out_of_range"] = (clean_dti < 0) | (clean_dti > 1)

    approved = _to_bool(_column_or_na(df, "clean_loan_approved")).eq(True)
    rejected = _to_bool(_column_or_na(df, "clean_loan_approved")).eq(False)
    approved_amount_blank = _blank_mask(_column_or_na(df, "clean_approved_amount"))
    interest_blank = _blank_mask(_column_or_na(df, "clean_interest_rate"))
    rejection_blank = _blank_mask(_column_or_na(df, "clean_rejection_reason"))
    flags["flag_approved_missing_required_fields"] = approved & (
        approved_amount_blank | interest_blank
    )
    flags["flag_rejected_missing_reason"] = rejected & rejection_blank

    flags["flag_approved_credit_history_zero"] = approved & clean_credit_history.eq(0)
    flags["flag_approved_credit_history_lt6"] = approved & (clean_credit_history < 6)
    flags["flag_private_ip_address"] = _column_or_na(df, "raw_applicant_ip_address").apply(_is_private_ip)

    return flags.fillna(False).astype(bool)


def validate_spending_postclean(df: pd.DataFrame) -> pd.DataFrame:
    """Compute post-clean spending validation flags using cleaned columns."""
    flags = pd.DataFrame(index=df.index)
    category_clean = _column_or_na(df, "category_clean")
    flags["flag_spending_missing_category"] = _blank_mask(category_clean)

    if "amount_non_numeric_flag" in df.columns:
        flags["flag_spending_amount_non_numeric"] = df["amount_non_numeric_flag"].fillna(False).astype(bool)
    else:
        raw_amount = _column_or_na(df, "raw_amount")
        amount_blank = _blank_mask(raw_amount)
        amount_num = _to_numeric(raw_amount)
        flags["flag_spending_amount_non_numeric"] = (~amount_blank) & amount_num.isna()

    clean_amount = _to_numeric(_column_or_na(df, "amount_clean"))
    flags["flag_spending_amount_negative"] = clean_amount < 0
    return flags.fillna(False).astype(bool)


def summarize_validation_flags(
    flags: pd.DataFrame,
    rules: dict[str, RuleDef],
    application_ids: pd.Series,
    stage: str,
) -> pd.DataFrame:
    """Aggregate row-level validation flags into counts, rates, and examples."""
    rows: list[dict[str, Any]] = []
    denominator = len(flags.index)
    for flag_col, rule in rules.items():
        if flag_col not in flags.columns:
            continue
        mask = flags[flag_col].fillna(False).astype(bool)
        failed_count = int(mask.sum())
        failed_percent = float((failed_count / denominator) * 100) if denominator else 0.0
        examples = (
            application_ids[mask]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .sort_values()
            .head(5)
            .tolist()
        )
        rows.append(
            {
                "stage": stage,
                "rule_id": rule.rule_id,
                "field_path": rule.field_path,
                "description": rule.description,
                "failed_count": failed_count,
                "failed_percent": round(failed_percent, 2),
                "example_application_ids": "|".join(examples),
            }
        )
    return pd.DataFrame(rows)
