"""
governance.py — GDPR compliance, AI Act classification, and governance controls
for the NovaCred Credit Application Governance Analysis.

Role: Governance Officer
Inputs: curated_full_df (applications_curated_full.csv)
        analysis_df    (applications_analysis.csv)
        quality_report (data_quality_report.csv)

All public functions return plain DataFrames or dicts so results can be
inspected, printed, or persisted directly from the notebook.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


# ── Constants ──────────────────────────────────────────────────────────────────

# GDPR Article references used throughout
GDPR_ARTICLES: dict[str, str] = {
    "Art. 4(1)":    "Definition of personal data",
    "Art. 5(1)(a)": "Lawfulness, fairness and transparency",
    "Art. 5(1)(b)": "Purpose limitation",
    "Art. 5(1)(c)": "Data minimisation",
    "Art. 5(1)(e)": "Storage limitation",
    "Art. 6":       "Lawfulness of processing — lawful basis",
    "Art. 7":       "Conditions for consent",
    "Art. 9":       "Processing of special category data",
    "Art. 13":      "Information to be provided (data collected from subject)",
    "Art. 14":      "Information to be provided (data not from subject)",
    "Art. 17":      "Right to erasure ('right to be forgotten')",
    "Art. 22":      "Automated individual decision-making, including profiling",
    "Art. 25":      "Data protection by design and by default",
    "Art. 30":      "Records of processing activities (ROPA)",
    "Art. 32":      "Security of processing",
    "Art. 35":      "Data protection impact assessment (DPIA)",
}

# EU AI Act references
AI_ACT_ARTICLES: dict[str, str] = {
    "Annex III §5(b)":   "High-risk AI: creditworthiness assessment and credit scoring",
    "Art. 9":            "Risk management system — ongoing throughout lifecycle",
    "Art. 10":           "Data and data governance requirements for high-risk AI",
    "Art. 13":           "Transparency and provision of information to deployers",
    "Art. 14":           "Human oversight obligations",
    "Art. 26":           "Obligations of deployers of high-risk AI systems",
    "Art. 72":           "Registration in the EU database of high-risk AI systems",
}

# PII field catalogue with classification and GDPR mapping
PII_FIELD_CATALOGUE: list[dict[str, str]] = [
    {
        "field_path":       "applicant_info.full_name",
        "classification":   "Direct PII",
        "gdpr_category":    "Personal data",
        "gdpr_article":     "Art. 4(1), Art. 5(1)(c)",
        "risk":             "High",
        "recommendation":   "Pseudonymise or redact; retain only in secure audit log",
    },
    {
        "field_path":       "applicant_info.email",
        "classification":   "Direct PII",
        "gdpr_category":    "Personal data",
        "gdpr_article":     "Art. 4(1), Art. 5(1)(c)",
        "risk":             "High",
        "recommendation":   "Pseudonymise; mask domain for analytics",
    },
    {
        "field_path":       "applicant_info.ssn",
        "classification":   "Direct PII",
        "gdpr_category":    "Personal data (national identifier equivalent)",
        "gdpr_article":     "Art. 4(1), Art. 5(1)(c), Art. 32",
        "risk":             "Critical",
        "recommendation":   "Encrypt at rest; pseudonymise for all processing beyond identity verification",
    },
    {
        "field_path":       "applicant_info.ip_address",
        "classification":   "Direct PII",
        "gdpr_category":    "Online identifier — personal data per GDPR recital 30",
        "gdpr_article":     "Art. 4(1), Art. 5(1)(c)",
        "risk":             "High",
        "recommendation":   "Retain only for fraud/security purposes; apply storage limitation",
    },
    {
        "field_path":       "applicant_info.date_of_birth",
        "classification":   "Quasi-PII",
        "gdpr_category":    "Personal data (temporal identifier — identifies when combined)",
        "gdpr_article":     "Art. 5(1)(c)",
        "risk":             "Medium",
        "recommendation":   "Convert to age band for analytics; retain raw only in secure store",
    },
    {
        "field_path":       "applicant_info.gender",
        "classification":   "Quasi-PII",
        "gdpr_category":    "Personal data",
        "gdpr_article":     "Art. 5(1)(c)",
        "risk":             "Medium",
        "recommendation":   "Retain for fairness monitoring only; document lawful basis",
    },
    {
        "field_path":       "applicant_info.zip_code",
        "classification":   "Quasi-PII",
        "gdpr_category":    "Personal data (location proxy)",
        "gdpr_article":     "Art. 5(1)(c)",
        "risk":             "Medium",
        "recommendation":   "Monitor as potential proxy variable for protected characteristics",
    },
    {
        "field_path":       "spending_behavior[].category + amount",
        "classification":   "Behavioural data",
        "gdpr_category":    "Personal data — behavioural profiling",
        "gdpr_article":     "Art. 5(1)(b), Art. 22",
        "risk":             "High",
        "recommendation":   "Document purpose limitation; assess whether profiling triggers Art. 22",
    },
    {
        "field_path":       "decision.loan_approved + interest_rate",
        "classification":   "Decision output",
        "gdpr_category":    "Automated decision affecting data subject",
        "gdpr_article":     "Art. 22, Art. 13",
        "risk":             "Critical",
        "recommendation":   "Provide explanation mechanism; ensure human oversight for adverse decisions",
    },
]

# Governance gap definitions — fields/mechanisms expected but absent from dataset
GOVERNANCE_GAPS: list[dict[str, str]] = [
    {
        "gap_id":       "GAP-001",
        "gap_name":     "Missing consent timestamp",
        "field":        "consent_timestamp",
        "gdpr_article": "Art. 6, Art. 7",
        "severity":     "Critical",
        "description":  "No record of when or whether the applicant provided consent for data processing. Without this, NovaCred cannot demonstrate a lawful basis for processing personal data.",
        "recommendation": "Implement a consent capture mechanism at application intake. Log timestamp, consent version, and channel. Store in an immutable audit log.",
    },
    {
        "gap_id":       "GAP-002",
        "gap_name":     "Missing data retention policy",
        "field":        "retention_until",
        "gdpr_article": "Art. 5(1)(e)",
        "severity":     "High",
        "description":  "No retention deadline is stored per record. Data may be held indefinitely, violating the storage limitation principle.",
        "recommendation": "Define retention periods by data category (e.g., 5 years for approved applications, 2 years for rejections). Add a retention_until field populated at intake. Implement automated deletion or anonymisation at expiry.",
    },
    {
        "gap_id":       "GAP-003",
        "gap_name":     "Missing data source / transparency field",
        "field":        "data_source",
        "gdpr_article": "Art. 14",
        "severity":     "High",
        "description":  "No field indicates where applicant data was obtained. Where data is not collected directly from the subject, Art. 14 requires the controller to inform the subject of the data origin.",
        "recommendation": "Add a data_source field (e.g., 'direct_application', 'credit_bureau', 'partner_referral'). Include data provenance in the Privacy Notice.",
    },
    {
        "gap_id":       "GAP-004",
        "gap_name":     "Missing processing purpose field",
        "field":        "processing_purpose",
        "gdpr_article": "Art. 5(1)(b)",
        "severity":     "High",
        "description":  "No field documents the lawful purpose for which this record is being processed, making it impossible to enforce purpose limitation.",
        "recommendation": "Add a processing_purpose field with controlled vocabulary (e.g., 'credit_assessment', 'fraud_detection', 'regulatory_compliance'). Reject downstream use outside declared purposes.",
    },
    {
        "gap_id":       "GAP-005",
        "gap_name":     "No audit trail for automated decisions",
        "field":        "decision_audit_log",
        "gdpr_article": "Art. 22, Art. 13",
        "severity":     "Critical",
        "description":  "Credit decisions appear to be fully automated with no human review record and no explanation of the factors that drove the outcome. Art. 22 grants data subjects the right not to be subject to solely automated decisions with significant effects, unless specific conditions are met.",
        "recommendation": "Log the model version, feature weights, and decision rationale for every application. Implement a human-in-the-loop review process for borderline or adverse decisions. Provide applicants with a meaningful explanation upon request.",
    },
    {
        "gap_id":       "GAP-006",
        "gap_name":     "SSN stored unencrypted",
        "field":        "applicant_info.ssn",
        "gdpr_article": "Art. 25, Art. 32",
        "severity":     "Critical",
        "description":  "Social Security Numbers are stored in plain text in the raw dataset. This violates data protection by design and by default, and creates significant breach risk.",
        "recommendation": "Encrypt SSNs at rest using AES-256. Pseudonymise for all analytical use (SHA-256 with salted hash — already implemented in privacy.py). Restrict access to raw SSN to identity verification processes only.",
    },
    {
        "gap_id":       "GAP-007",
        "gap_name":     "No human oversight documentation",
        "field":        "human_review_flag",
        "gdpr_article": "Art. 22, Art. 14 (AI Act)",
        "severity":     "High",
        "description":  "There is no field indicating whether a human reviewed the automated decision, nor any mechanism for applicants to request human review. This is required under both GDPR Art. 22 and EU AI Act Art. 14.",
        "recommendation": "Add a human_review_flag and human_reviewer_id to the decision object. Implement an escalation pathway for applicants to contest automated decisions.",
    },
    {
        "gap_id":       "GAP-008",
        "gap_name":     "Sensitive behavioural data collected without explicit purpose",
        "field":        "spending_behavior",
        "gdpr_article": "Art. 5(1)(b), Art. 22",
        "severity":     "Medium",
        "description":  "Detailed spending behaviour (categories and amounts) is collected and could be used for profiling. If spending data influences credit decisions, this constitutes profiling under GDPR Art. 4(4) and may trigger Art. 22 obligations.",
        "recommendation": "Document whether spending_behavior influences the credit model. If so, disclose this in the Privacy Notice and provide an opt-out mechanism. Assess whether a DPIA (Art. 35) is required.",
    },
]


# ── PII Inventory ─────────────────────────────────────────────────────────────

def build_pii_catalogue() -> pd.DataFrame:
    """
    Return the full PII field catalogue as a DataFrame.
    Maps each field to its classification, GDPR article, risk level,
    and recommended control.
    """
    return pd.DataFrame(PII_FIELD_CATALOGUE)


# ── GDPR Gap Analysis ─────────────────────────────────────────────────────────

def build_gdpr_gap_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect governance fields that are expected but absent from the dataset.

    For each known governance gap, checks whether the field exists and is
    populated in the provided DataFrame. Returns a report DataFrame with
    presence status, GDPR article, severity, and recommended remediation.

    Parameters
    ----------
    df : pd.DataFrame
        The raw flattened applications DataFrame (applications_curated_full.csv
        or the raw flattened output).
    """
    rows: list[dict[str, Any]] = []
    for gap in GOVERNANCE_GAPS:
        field = gap["field"]
        # Check if the field exists in the DataFrame at all
        field_exists = field in df.columns
        if field_exists:
            # Check if it has any non-null, non-empty values
            populated = df[field].notna().any() and df[field].astype(str).str.strip().ne("").any()
            status = "Present & populated" if populated else "Present but empty"
        else:
            status = "Absent from dataset"

        rows.append(
            {
                "gap_id":         gap["gap_id"],
                "gap_name":       gap["gap_name"],
                "field":          field,
                "status":         status,
                "gdpr_article":   gap["gdpr_article"],
                "severity":       gap["severity"],
                "description":    gap["description"],
                "recommendation": gap["recommendation"],
            }
        )

    severity_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    report = pd.DataFrame(rows)
    report["_sev_order"] = report["severity"].map(severity_order).fillna(99)
    report = report.sort_values("_sev_order").drop(columns="_sev_order").reset_index(drop=True)
    return report


def gdpr_gap_summary(gap_report: pd.DataFrame) -> pd.DataFrame:
    """
    Compact summary of gap counts by severity — suitable for the README table.
    """
    return (
        gap_report.groupby("severity", observed=True)["gap_id"]
        .count()
        .reindex(["Critical", "High", "Medium", "Low"], fill_value=0)
        .rename("gap_count")
        .reset_index()
        .rename(columns={"severity": "Severity", "gap_count": "Number of Gaps"})
    )


# ── MongoDB Audit Query Simulation ────────────────────────────────────────────

def mongo_query_1_duplicate_ssns(df: pd.DataFrame, ssn_col: str = "raw_applicant_ssn") -> pd.DataFrame:
    """
    Pandas equivalent of MongoDB Audit Query 1 (Uniqueness):

        db.credit_applications.aggregate([
          { $group: { _id: "$applicant_info.ssn",
                      count: { $sum: 1 },
                      names: { $push: "$applicant_info.full_name" } }},
          { $match: { count: { $gt: 1 } } },
          { $sort:  { count: -1 } }
        ])

    Returns duplicate SSN groups with application counts.
    """
    if ssn_col not in df.columns:
        return pd.DataFrame(columns=["ssn", "count", "application_ids"])

    ssn = df[ssn_col].fillna("").astype(str).str.strip()
    non_blank = df[ssn.ne("")]
    grouped = (
        non_blank.groupby(ssn_col)
        .agg(count=("application_id", "count"),
             application_ids=("application_id", lambda x: "|".join(x.astype(str).tolist())))
        .reset_index()
        .rename(columns={ssn_col: "ssn"})
    )
    duplicates = grouped[grouped["count"] > 1].sort_values("count", ascending=False).reset_index(drop=True)
    return duplicates


def mongo_query_2_gender_consistency(df: pd.DataFrame, gender_col: str = "raw_applicant_gender") -> pd.DataFrame:
    """
    Pandas equivalent of MongoDB Audit Query 2 (Consistency):

        db.credit_applications.aggregate([
          { $group: { _id: "$applicant_info.gender", count: { $sum: 1 } }},
          { $sort:  { count: -1 } }
        ])

    Returns all distinct gender values and their frequency, exposing
    inconsistent encodings (e.g., 'm', 'M', 'Male', 'male').
    """
    if gender_col not in df.columns:
        return pd.DataFrame(columns=["gender_value", "count"])

    result = (
        df[gender_col]
        .fillna("[NULL]")
        .astype(str)
        .str.strip()
        .value_counts(dropna=False)
        .rename_axis("gender_value")
        .reset_index()
        .rename(columns={gender_col: "count", "count": "count"})
    )
    result.columns = ["gender_value", "count"]
    return result


def mongo_query_3_missing_consent(df: pd.DataFrame) -> dict[str, Any]:
    """
    Pandas equivalent of MongoDB Audit Query 3 (Completeness / GDPR gap):

        db.credit_applications.aggregate([
          { $match:  { "consent_timestamp": { $exists: false } }},
          { $count: "missing_consent" }
        ])

    Also checks for retention_until, data_source, and processing_purpose
    as per the governance fields listed in the lecture slides.
    """
    governance_fields = {
        "consent_timestamp":  "Art. 6 / Art. 7 — Lawful basis / Consent",
        "retention_until":    "Art. 5(1)(e) — Storage limitation",
        "data_source":        "Art. 14 — Transparency",
        "processing_purpose": "Art. 5(1)(b) — Purpose limitation",
    }
    total = len(df)
    results: dict[str, Any] = {"total_records": total, "fields": {}}

    for field, gdpr_ref in governance_fields.items():
        if field not in df.columns:
            missing_count = total
            present_count = 0
        else:
            missing_count = int(df[field].isna().sum() + (df[field].astype(str).str.strip().eq("")).sum())
            present_count = total - missing_count

        results["fields"][field] = {
            "gdpr_reference":  gdpr_ref,
            "present_count":   present_count,
            "missing_count":   missing_count,
            "missing_percent": round((missing_count / total) * 100 if total > 0 else 0.0, 2),
            "status":          "ABSENT" if field not in df.columns else ("EMPTY" if missing_count == total else "PARTIAL/OK"),
        }

    return results


def mongo_query_4_validity(df: pd.DataFrame) -> dict[str, Any]:
    """
    Pandas equivalent of MongoDB Audit Query 4 (Validity):

        // Negative income
        db.credit_applications.aggregate([
          { $match: { "financials.annual_income": { $lt: 0 } }},
          { $count: "negative_income" }
        ])

        // Unrealistic DTI
        db.credit_applications.aggregate([
          { $match: { "financials.debt_to_income": { $gt: 1 } }},
          { $project: { _id: 1, dti: "$financials.debt_to_income" }}
        ])
    """
    import numpy as np

    results: dict[str, Any] = {}
    total = len(df)

    # Negative income check
    if "raw_financial_annual_income" in df.columns:
        income = pd.to_numeric(df["raw_financial_annual_income"], errors="coerce")
        neg_mask = income < 0
        results["negative_income"] = {
            "count":   int(neg_mask.sum()),
            "percent": round((neg_mask.sum() / total) * 100 if total > 0 else 0.0, 2),
            "rule":    "Validity — income cannot be negative",
        }

    # DTI out of [0, 1] range
    if "raw_financial_debt_to_income" in df.columns:
        dti = pd.to_numeric(df["raw_financial_debt_to_income"], errors="coerce")
        dti_invalid = (dti < 0) | (dti > 1)
        results["dti_out_of_range"] = {
            "count":   int(dti_invalid.sum()),
            "percent": round((dti_invalid.sum() / total) * 100 if total > 0 else 0.0, 2),
            "rule":    "Validity — debt-to-income must be in [0, 1]",
        }

    # Negative credit history
    if "raw_financial_credit_history_months" in df.columns:
        ch = pd.to_numeric(df["raw_financial_credit_history_months"], errors="coerce")
        neg_ch = ch < 0
        results["negative_credit_history"] = {
            "count":   int(neg_ch.sum()),
            "percent": round((neg_ch.sum() / total) * 100 if total > 0 else 0.0, 2),
            "rule":    "Validity — credit history months cannot be negative",
        }

    # Negative savings
    if "raw_financial_savings_balance" in df.columns:
        sav = pd.to_numeric(df["raw_financial_savings_balance"], errors="coerce")
        neg_sav = sav < 0
        results["negative_savings"] = {
            "count":   int(neg_sav.sum()),
            "percent": round((neg_sav.sum() / total) * 100 if total > 0 else 0.0, 2),
            "rule":    "Validity — savings balance cannot be negative",
        }

    return results


def mongo_query_5_bias_approval_rate(df: pd.DataFrame,
                                      gender_col: str = "raw_applicant_gender",
                                      approved_col: str = "raw_decision_loan_approved") -> pd.DataFrame:
    """
    Pandas equivalent of MongoDB Audit Query 5 (AI Act — Fairness Testing):

        db.credit_applications.aggregate([
          { $group: {
              _id: "$applicant_info.gender",
              total:    { $sum: 1 },
              approved: { $sum: { $cond: ["$decision.loan_approved", 1, 0] }}
          }},
          { $addFields: {
              approval_rate: { $divide: ["$approved", "$total"] }
          }},
          { $sort: { approval_rate: -1 } }
        ])

    Computes approval rate by raw gender value (before normalisation),
    matching the MongoDB aggregation pattern from the lecture slides.
    """
    if gender_col not in df.columns or approved_col not in df.columns:
        return pd.DataFrame()

    approved_bool = df[approved_col].map(
        {"true": True, "false": False, True: True, False: False, 1: True, 0: False,
         "True": True, "False": False, "1": True, "0": False}
    )

    temp = df[[gender_col]].copy()
    temp["approved_int"] = approved_bool.map({True: 1, False: 0})
    temp["gender_raw"] = temp[gender_col].fillna("[NULL]").astype(str).str.strip()

    result = (
        temp.groupby("gender_raw")
        .agg(total=("approved_int", "count"),
             approved=("approved_int", "sum"))
        .assign(approval_rate=lambda x: (x["approved"] / x["total"]).round(4))
        .sort_values("approval_rate", ascending=False)
        .reset_index()
    )
    return result


# ── AI Act Classification ─────────────────────────────────────────────────────

def build_ai_act_classification() -> dict[str, Any]:
    """
    Return NovaCred's AI Act classification and associated obligations.

    NovaCred's credit scoring system is HIGH-RISK under EU AI Act Annex III §5(b):
    'AI systems intended to be used for creditworthiness assessment or credit
    scoring of natural persons.'

    Returns a dict with risk level, legal basis, and a list of obligations.
    """
    return {
        "system_name":    "NovaCred Automated Credit Scoring System",
        "risk_level":     "HIGH-RISK",
        "legal_basis":    "EU AI Act Annex III, Point 5(b) — Creditworthiness assessment and credit scoring",
        "obligations": [
            {
                "article":     "Art. 9",
                "obligation":  "Risk management system",
                "description": "Implement and maintain a risk management system throughout the entire lifecycle of the AI system. Identify and analyse known and foreseeable risks.",
            },
            {
                "article":     "Art. 10",
                "obligation":  "Data governance",
                "description": "Training, validation, and testing data must meet quality criteria. Data must be examined for biases. Data gaps and shortcomings must be addressed.",
            },
            {
                "article":     "Art. 13",
                "obligation":  "Transparency",
                "description": "The AI system must be sufficiently transparent to enable deployers to interpret its output. A technical document must be produced and kept up to date.",
            },
            {
                "article":     "Art. 14",
                "obligation":  "Human oversight",
                "description": "High-risk AI systems must be designed to allow effective human oversight. Humans must be able to intervene, override, or stop the system.",
            },
            {
                "article":     "Art. 26",
                "obligation":  "Deployer obligations",
                "description": "NovaCred (as deployer) must ensure the system is used in accordance with instructions, monitor operation, and inform the provider of risks identified.",
            },
            {
                "article":     "Art. 72",
                "obligation":  "EU database registration",
                "description": "High-risk AI systems in scope of Annex III must be registered in the EU database before being placed on the market or put into service.",
            },
            {
                "article":     "Art. 35 (GDPR)",
                "obligation":  "DPIA required",
                "description": "Automated credit decisions using profiling likely require a Data Protection Impact Assessment under GDPR Art. 35 before processing begins.",
            },
        ],
        "immediate_gaps": [
            "No human oversight mechanism documented in the dataset",
            "No audit trail linking decisions to model version or feature inputs",
            "No DPIA evidence present",
            "No EU AI Act registration recorded",
            "Fairness testing shows DI = 0.77 (below 0.80 threshold) — potential Art. 10 violation",
        ],
    }


def build_ai_act_summary_df(ai_act: dict[str, Any]) -> pd.DataFrame:
    """Convert the AI Act classification dict to a tidy DataFrame for display."""
    return pd.DataFrame(ai_act["obligations"])


# ── Governance Recommendations ────────────────────────────────────────────────

def build_governance_recommendations() -> pd.DataFrame:
    """
    Return a prioritised, actionable governance recommendations table.

    Each row represents one control with its priority, category,
    GDPR/AI Act reference, effort estimate, and responsible role.
    """
    recommendations = [
        {
            "priority":       1,
            "control_id":     "GOV-001",
            "category":       "Legal Compliance",
            "title":          "Implement consent capture and tracking",
            "description":    "Add consent_timestamp, consent_version, and consent_channel to the application intake form. Store in an immutable log. Required to demonstrate lawful basis under GDPR Art. 6/7.",
            "legal_ref":      "GDPR Art. 6, Art. 7",
            "effort":         "Medium",
            "responsible":    "Engineering + Legal",
        },
        {
            "priority":       2,
            "control_id":     "GOV-002",
            "category":       "Legal Compliance",
            "title":          "Define and enforce data retention policy",
            "description":    "Establish retention periods per data category. Populate retention_until at intake. Automate deletion or anonymisation at expiry. Document in ROPA (Art. 30).",
            "legal_ref":      "GDPR Art. 5(1)(e), Art. 30",
            "effort":         "Medium",
            "responsible":    "Data Engineering + DPO",
        },
        {
            "priority":       3,
            "control_id":     "GOV-003",
            "category":       "Security",
            "title":          "Encrypt and pseudonymise SSNs",
            "description":    "Encrypt SSNs at rest (AES-256). Use salted SHA-256 hash for all analytical processing (already implemented in privacy.py). Restrict raw SSN access to identity verification only.",
            "legal_ref":      "GDPR Art. 25, Art. 32",
            "effort":         "Low",
            "responsible":    "Data Engineering",
        },
        {
            "priority":       4,
            "control_id":     "GOV-004",
            "category":       "AI Act / Automated Decisions",
            "title":          "Implement human oversight mechanism",
            "description":    "Add human_review_flag and human_reviewer_id to the decision schema. Require mandatory human review for adverse decisions and borderline cases. Provide an appeal pathway for applicants.",
            "legal_ref":      "GDPR Art. 22, EU AI Act Art. 14",
            "effort":         "High",
            "responsible":    "Product + Operations",
        },
        {
            "priority":       5,
            "control_id":     "GOV-005",
            "category":       "AI Act / Automated Decisions",
            "title":          "Create decision audit trail",
            "description":    "Log model version, feature values, and decision rationale for every application. Retain audit log for minimum 5 years. Required for explainability and Art. 22 compliance.",
            "legal_ref":      "GDPR Art. 22, EU AI Act Art. 13",
            "effort":         "Medium",
            "responsible":    "Data Science + Engineering",
        },
        {
            "priority":       6,
            "control_id":     "GOV-006",
            "category":       "Fairness",
            "title":          "Address gender disparate impact (DI = 0.77)",
            "description":    "DI ratio of 0.77 is below the four-fifths threshold. Investigate whether financial proxy variables (income, credit history) are driving the gap. Implement fairness constraints in the model or adjust decision thresholds.",
            "legal_ref":      "EU AI Act Art. 10, GDPR Art. 22",
            "effort":         "High",
            "responsible":    "Data Science + Legal",
        },
        {
            "priority":       7,
            "control_id":     "GOV-007",
            "category":       "Transparency",
            "title":          "Add data source and processing purpose fields",
            "description":    "Populate data_source and processing_purpose at intake. Include data provenance in Privacy Notice. Required where data is not collected directly from the subject.",
            "legal_ref":      "GDPR Art. 5(1)(b), Art. 14",
            "effort":         "Low",
            "responsible":    "Engineering + Legal",
        },
        {
            "priority":       8,
            "control_id":     "GOV-008",
            "category":       "AI Act",
            "title":          "Conduct DPIA and register with EU AI Act database",
            "description":    "Automated credit scoring using profiling requires a DPIA under GDPR Art. 35. As a high-risk AI system under Annex III §5(b), NovaCred must also register in the EU AI Act database before deployment.",
            "legal_ref":      "GDPR Art. 35, EU AI Act Art. 72",
            "effort":         "High",
            "responsible":    "DPO + Legal + Management",
        },
        {
            "priority":       9,
            "control_id":     "GOV-009",
            "category":       "Data Quality",
            "title":          "Standardise gender encoding at source",
            "description":    "Enforce a controlled vocabulary for gender at intake (Male / Female / Prefer not to say). Reject non-conforming values at the API layer. Eliminates the consistency issue identified in data quality audit.",
            "legal_ref":      "GDPR Art. 5(1)(d) — Accuracy",
            "effort":         "Low",
            "responsible":    "Data Engineering",
        },
        {
            "priority":       10,
            "control_id":     "GOV-010",
            "category":       "Privacy by Design",
            "title":          "Apply data minimisation to spending behaviour data",
            "description":    "Assess whether all spending categories are necessary for the credit decision. Remove or aggregate categories that do not improve model performance. Document the necessity assessment.",
            "legal_ref":      "GDPR Art. 5(1)(c), Art. 25",
            "effort":         "Medium",
            "responsible":    "Data Science + DPO",
        },
    ]
    return pd.DataFrame(recommendations)


# ── Pseudonymisation Evidence ─────────────────────────────────────────────────

def pseudonymisation_evidence(curated_df: pd.DataFrame, analysis_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a before/after pseudonymisation comparison table for the notebook.
    Shows which PII fields are present in the curated layer vs removed in analysis.
    """
    pii_fields = {
        "raw_applicant_full_name":   "Full name",
        "raw_applicant_email":       "Email address",
        "raw_applicant_ssn":         "Social Security Number",
        "raw_applicant_ip_address":  "IP address",
        "raw_applicant_date_of_birth": "Date of birth",
    }
    pseudonymised_fields = {
        "applicant_pseudo_id": "SHA-256 pseudonym (salt + SSN/email/name+DOB+zip)",
        "age_band":            "Age band (coarse — not exact DOB)",
        "clean_gender":        "Gender (kept for fairness monitoring only)",
    }

    rows = []
    for field, label in pii_fields.items():
        rows.append({
            "field":            label,
            "raw_field_name":   field,
            "in_curated_layer": field in curated_df.columns,
            "in_analysis_layer": False,
            "treatment":        "Removed from analysis dataset",
            "gdpr_principle":   "Data minimisation (Art. 5(1)(c))",
        })
    for field, desc in pseudonymised_fields.items():
        rows.append({
            "field":            desc,
            "raw_field_name":   f"→ {field}",
            "in_curated_layer": False,
            "in_analysis_layer": field in analysis_df.columns,
            "treatment":        "Derived / pseudonymised replacement",
            "gdpr_principle":   "Privacy by design (Art. 25)",
        })

    return pd.DataFrame(rows)
    
