# Current Status + todos

- @madalena adding key insights to readme

- @all finalising ppt with key insights, quantified + visualization until 07.03.

- @all recording video on Canva until 07.03.

**next meeting 07.03. 10:30 - AGENDA**

- Review and arrange github structure

- Reviewing questions

- Reviewing ppt

- Recording video

# NovaCred — Credit Application Governance Analysis

> DEGO 2606 Group Project – Credit Application Governance Analysis  
> MSc Business Analytics | Nova SBE | 2025/26 T3

---

## 👥 Team

| Name           | Role               | GitHub                | Contributions                                                                                             |
| -------------- | ------------------ | --------------------- | --------------------------------------------------------------------------------------------------------- |
| Connor Brown   | Data Engineer      | @Connor144-iv         | Data Quality Notebook + Output, Key Findings in README, Data Preparation for further analysis             |
| Madalena Rocha | Data Scientist     | @msusanrocha          | GitHub Setup, Bias Analysis Notebook + Output, Key Findings in README, Governance Support                 |
| António Santos | Governance Officer | @antonioncmsantos-hue | Privacy Demo Notebook + Output, Key Findings in README, Governance Recommendations                        |
| Nora Puchert   | Product Lead       | @npu99                | Progress + structure tracking, Meeting organisation, README curation, Requirements guidance, Presentation |

---

## 📌 Executive Summary

This report presents the findings of a data governance audit of NovaCred's credit application dataset, covering 502 records across data quality, algorithmic fairness, and regulatory compliance.

Issues were identified across all three dimensions. On data quality, the most significant structural gap is that 87.65% of records (440 of 502) are missing a processing timestamp, which limits auditability across the majority of decisions and undermines any deterministic record selection where duplicates exist. Beyond completeness, the raw dataset contained inconsistent gender encoding across 22.1% of records, non-standard date formats in 31.3%, a field naming inconsistency between `annual_income` and `annual_salary`, and a small number of invalid financial values including negative credit history months and a debt-to-income ratio above 1.0. Two duplicate application IDs were found, one of which is a conflict duplicate with materially contradictory fields. All remediable defects were resolved in the pipeline with full audit lineage preserved through separate raw and clean columns.

On fairness, the gender Disparate Impact ratio is **0.77**, falling below the four-fifths legal threshold of 0.80. A chi-squared test confirms the gap is statistically significant. No age band triggered the same threshold, though the correlation between age and credit history length is noted as a monitoring point for proxy discrimination.

On governance, the audit identified **8 GDPR compliance gaps**, of which 3 are critical. These include the absence of any consent documentation across all 502 records, no data retention policy, and SSNs stored in plain text, which would trigger mandatory individual notification under GDPR Art. 34 in the event of a breach. The credit scoring system is classified **HIGH-RISK under EU AI Act Annex III §5(b)**, triggering 7 compliance obligations, the majority of which show no evidence of implementation. No DPIA has been conducted and the system has not been registered in the EU AI Act database.

One finding connects all three areas. 81.6% of loan rejections cite only `algorithm_risk_score` as the sole reason with no further explanation, making it impossible to explain individual decisions to applicants as required under GDPR Art. 22 and preventing any meaningful investigation into the source of the documented gender disparity.

The 10 governance recommendations in this report are sequenced by legal urgency, with consent capture, SSN encryption, and a human oversight mechanism as the immediate priorities.

---

## 📁 Repository Structure

The project is organized into a modular hierarchy to ensure a clear audit trail from raw data to final reporting:

```
DEGO_PROJECT_GROUP03/
├── data/
│   ├── curated/                # Final, high-integrity datasets for modeling
│   ├── quality/                # Datasets with outputs and documentation
│   └── raw/                    # Original data source
├── figures/                    # Visualization assets and plots
├── notebooks/                  # Main Notebooks: Data Quality, Bias Analysis, Privacy Demo
├── presentation/               # Video link and presentation
├── reports/                    # Additional documentation and compliance outputs
│   └── quality/
│   └── governance/
└── src/                        # Modular codebase for scalability and reuse
```

---

## 🔍 Data Quality

## Objectives

1. Identify and document all data quality issues in the raw NovaCred credit application data.
2. Quantify the extent of each issue.
3. Demonstrate remediation steps and their impact.
4. Provide analysis ready deliverables.

## Data Quality Definitions

# Quality Issue Definition

| Issue Group       | Definition                                                                                  |
| ----------------- | ------------------------------------------------------------------------------------------- |
| Completeness      | Data required for business or regulatory purposes is missing, blank, or not populated.      |
| Validity          | Data values violate defined business rules, formats, or allowable ranges.                   |
| Consistency       | Data is present but not stored in a standard, canonical, or expected representation.        |
| Cross-field logic | Data fields are individually valid but logically inconsistent when evaluated together.      |
| Privacy           | Data contains sensitive attributes that require protection, masking, or governance control. |
| Uniqueness        | Records or identifiers that should be distinct appear more than once.                       |

# Quality Rule Definition

| Rule ID         | Definition                                                        |
| --------------- | ----------------------------------------------------------------- |
| R_APP_001       | Missing or blank processing timestamp.                            |
| R_APP_002       | One or more required applicant fields missing or blank.           |
| R_APP_003       | Both SSN and IP address are missing or blank.                     |
| R_APP_004       | Email is missing or blank.                                        |
| R_APP_005       | Email does not match the expected format.                         |
| R_APP_006       | Gender is not already in canonical form.                          |
| R_APP_007       | Gender is outside the allowed values.                             |
| R_APP_008       | Date of birth is not in canonical YYYY-MM-DD form.                |
| R_APP_009       | Date of birth matches an ambiguous NN/NN/YYYY pattern.            |
| R_APP_010       | Annual income is stored as a string or cannot be coerced cleanly. |
| R_APP_011       | Annual salary is populated instead of annual income.              |
| R_APP_012       | Credit history months is negative.                                |
| R_APP_013       | Savings balance is negative.                                      |
| R_APP_014       | Debt-to-income is outside the allowed range [0, 1].               |
| R_APP_015       | Approved loan is missing interest_rate and/or approved_amount.    |
| R_APP_016       | Rejected loan is missing rejection_reason.                        |
| R_APP_017       | Loan approved with zero months of credit history.                 |
| R_APP_018       | Loan approved with less than 6 months of credit history.          |
| R_APP_019       | IP address is in a private range and likely masked or synthetic.  |
| R_SPN_001       | Spending category is missing or blank.                            |
| R_SPN_002       | Spending amount cannot be parsed as numeric.                      |
| R_SPN_003       | Spending amount is negative.                                      |
| R_DUP_001       | Rows with duplicated application_id values.                       |
| R_DUP_002       | Distinct application_id values that are duplicated.               |
| R_DUP_003       | Rows where SSN repeats across one or more records.                |
| R_DUP_004       | Distinct SSN values that appear across different application IDs. |
| R_DUP_CONFLICT  | Duplicated application IDs classified as conflicts.               |
| R_DUP_CANONICAL | Canonical application rows retained for downstream analysis.      |

## Data Quality Findings

| issue_group       | rule_id         | metric_label                                                                                                           |   pre_clean_count |   post_clean_count |   delta_count |   pre_clean_percent |   post_clean_percent |   delta_percent |
|:------------------|:----------------|:-----------------------------------------------------------------------------------------------------------------------|------------:|-------------:|--------------:|--------------:|---------------:|----------------:|
| Completeness      | R_APP_001       | Missing or blank processing timestamp.                                                                                 |         440 |          440 |             0 |         87.65 |          87.65 |            0    |
| Completeness      | R_APP_002       | One or more required applicant fields missing or blank.                                                                |           8 |            8 |             0 |          1.59 |           1.59 |            0    |
| Completeness      | R_APP_003       | Both SSN and IP address are missing or blank.                                                                          |           5 |            5 |             0 |          1    |           1    |            0    |
| Completeness      | R_APP_004       | Email is missing or blank.                                                                                             |           7 |            7 |             0 |          1.39 |           1.39 |            0    |
| Validity          | R_APP_005       | Email does not match the expected format.                                                                              |           4 |            4 |             0 |          0.8  |           0.8  |            0    |
| Consistency       | R_APP_006       | Gender is not already in canonical form.                                                                               |         111 |            0 |          -111 |         22.11 |           0    |          -22.11 |
| Validity          | R_APP_007       | Gender is outside the allowed values.                                                                                  |           0 |            0 |             0 |          0    |           0    |            0    |
| Consistency       | R_APP_008       | Date of birth is not in canonical YYYY-MM-DD form.                                                                     |         157 |            0 |          -157 |         31.27 |           0    |          -31.27 |
| Consistency       | R_APP_009       | Date of birth matches the ambiguous NN/NN/YYYY pattern. When DOB is NN/NN/YYYY and both NN <= 12, parse as MM/DD/YYYY. |          39 |           39 |             0 |          7.77 |           7.77 |            0    |
| Consistency       | R_APP_010       | Annual income is stored as a string or cannot be coerced cleanly.                                                      |           0 |            0 |             0 |          0    |           0    |            0    |
| Consistency       | R_APP_011       | Annual salary is populated instead of annual income.                                                                   |           5 |            5 |             0 |          1    |           1    |            0    |
| Validity          | R_APP_012       | Credit history months is negative.                                                                                     |           2 |            0 |            -2 |          0.4  |           0    |           -0.4  |
| Validity          | R_APP_013       | Savings balance is negative.                                                                                           |           1 |            0 |            -1 |          0.2  |           0    |           -0.2  |
| Validity          | R_APP_014       | Debt-to-income is outside the allowed range [0, 1].                                                                    |           1 |            0 |            -1 |          0.2  |           0    |           -0.2  |
| Cross-field logic | R_APP_015       | Approved loan is missing interest_rate and/or approved_amount.                                                         |           0 |            0 |             0 |          0    |           0    |            0    |
| Cross-field logic | R_APP_016       | Rejected loan is missing rejection_reason.                                                                             |           0 |            0 |             0 |          0    |           0    |            0    |
| Cross-field logic | R_APP_017       | Loan approved with zero months of credit history.                                                                      |          11 |           11 |             0 |          2.19 |           2.19 |            0    |
| Privacy           | R_APP_019       | IP address is in a private range and likely masked or synthetic.                                                       |         497 |          497 |             0 |         99    |          99    |            0    |
| Uniqueness        | R_DUP_001       | Rows with duplicated application_id values.                                                                            |           4 |            4 |             0 |          0.8  |           0.8  |            0    |
| Uniqueness        | R_DUP_002       | Distinct application_id values that are duplicated.                                                                    |           2 |            2 |             0 |          0.4  |           0.4  |            0    |
| Uniqueness        | R_DUP_003       | Rows where SSN repeats across one or more records.                                                                     |           6 |            6 |             0 |          1.2  |           1.2  |            0    |
| Uniqueness        | R_DUP_004       | Distinct SSN values that appear across different application IDs.                                                      |           2 |            2 |             0 |          0.4  |           0.4  |            0    |
| Remediation       | R_DUP_CANONICAL | Canonical rows retained for analysis                                                                                   |         502 |          500 |            -2 |        100    |          99.6  |           -0.4  |
| Uniqueness        | R_DUP_CONFLICT  | Duplicate conflict IDs                                                                                                 |           1 |            1 |             0 |          0.2  |           0.2  |            0    |
| Completeness      | R_SPN_001       | Spending category is missing or blank.                                                                                 |           0 |            0 |             0 |          0    |           0    |            0    |
| Validity          | R_SPN_002       | Spending amount cannot be parsed as numeric.                                                                           |           0 |            0 |             0 |          0    |           0    |            0    |
| Validity          | R_SPN_003       | Spending amount is negative.                                                                                           |           0 |            0 |             0 |          0    |           0    |            0    |

## Core Outputs

### `data/curated/`

- `applications_curated_full.csv`
  - Restricted audit dataset.
  - One row per raw record.
  - Contains raw columns, clean columns, a remediation flag set, and duplicate metadata.
- `applications_analysis.csv`
  - PII-safe analysis dataset.
  - One row per canonical application.
  - Contains only clean modelling and fairness features.
- `spending_items_clean.csv`
  - Cleaned spending-level analysis dataset.
  - One row per spending item.

### `data/quality/`

- `data_quality_report.csv`
  - Consolidated issue registry with both `pre` and `post` stages.
- `before_after_comparison.csv`
  - Compact remediation evidence table.
- `duplicate_id_report.csv`
  - Duplicate classification and canonical selection details.
- `rule_catalog.csv`
  - Minimal stage-aware rule reference.
- `pii_inventory.csv`
  - Minimal privacy inventory for raw, curated, and analysis datasets.

## Cleaning Choices

- Raw values are never overwritten.
- Cleaned values are stored in `clean_*` columns.
- Only a small set of remediation flags is retained in `applications_curated_full.csv`.
- DOB ambiguity rule:
  - For `NN/NN/YYYY` where both parts are `<= 12`, parse as `MM/DD/YYYY` and set `dob_ambiguous_flag=True`.
- `annual_salary` is mapped into `clean_annual_income` only when `annual_income` is missing.
- Negative `credit_history_months`, out-of-range `debt_to_income`, and negative `savings_balance` are nullified in the clean columns.


## Code Layout

- `src/config.py`: paths and project constants.
- `src/io_utils.py`: JSON loading and CSV writing helpers.
- `src/flatten.py`: JSON-to-table transforms.
- `src/schema.py`: rule definitions and validation functions.
- `src/quality.py`: duplicate analysis and compact reporting.
- `src/clean.py`: deterministic cleaning and standardisation.
- `src/privacy.py`: redaction, pseudonymisation, and PII-safe outputs.
- `notebooks/01-data-quality.ipynb`: orchestration notebook.

---

## ⚖️ Bias Detection & Fairness

---

## 🔐 Privacy & Governance

### PII Inventory & Classification

Under [GDPR Art. 30](https://gdpr-info.eu/art-30-gdpr/), controllers must maintain Records of Processing Activities documenting the categories of personal data processed.

| Field                                    | Classification   | Risk         | GDPR Article                                                                                    | Recommendation                                                     |
| ---------------------------------------- | ---------------- | ------------ | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `applicant_info.full_name`               | Direct PII       | High         | [Art. 4(1)](https://gdpr-info.eu/art-4-gdpr/), [Art. 5(1)(c)](https://gdpr-info.eu/art-5-gdpr/) | Pseudonymise or redact                                             |
| `applicant_info.email`                   | Direct PII       | High         | [Art. 4(1)](https://gdpr-info.eu/art-4-gdpr/), [Art. 5(1)(c)](https://gdpr-info.eu/art-5-gdpr/) | Pseudonymise; mask domain for analytics                            |
| `applicant_info.ssn`                     | Direct PII       | **Critical** | [Art. 4(1)](https://gdpr-info.eu/art-4-gdpr/), [Art. 32](https://gdpr-info.eu/art-32-gdpr/)     | Encrypt at rest; pseudonymise for all processing                   |
| `applicant_info.ip_address`              | Direct PII       | High         | [Art. 4(1)](https://gdpr-info.eu/art-4-gdpr/), [Art. 5(1)(c)](https://gdpr-info.eu/art-5-gdpr/) | Retain only for fraud/security; apply storage limitation           |
| `applicant_info.date_of_birth`           | Quasi-PII        | Medium       | [Art. 5(1)(c)](https://gdpr-info.eu/art-5-gdpr/)                                                | Convert to age band for analytics; retain raw only in secure store |
| `applicant_info.gender`                  | Quasi-PII        | Medium       | [Art. 5(1)(c)](https://gdpr-info.eu/art-5-gdpr/)                                                | Retain for fairness monitoring only                                |
| `applicant_info.zip_code`                | Quasi-PII        | Medium       | [Art. 5(1)(c)](https://gdpr-info.eu/art-5-gdpr/)                                                | Monitor as potential ethnic proxy                                  |
| `spending_behavior`                      | Behavioural data | High         | [Art. 5(1)(b)](https://gdpr-info.eu/art-5-gdpr/), [Art. 22](https://gdpr-info.eu/art-22-gdpr/)  | Document purpose limitation; assess Art. 22 profiling risk         |
| `decision.loan_approved + interest_rate` | Decision output  | **Critical** | [Art. 22](https://gdpr-info.eu/art-22-gdpr/), [Art. 13](https://gdpr-info.eu/art-13-gdpr/)      | Provide explanation mechanism; ensure human oversight              |

### Pseudonymisation Design

[GDPR Art. 25](https://gdpr-info.eu/art-25-gdpr/) requires data protection by design and by default. All direct PII is removed from the analysis layer and replaced with a deterministic SHA-256 pseudonym.

**Priority-based fallback strategy:**

| Priority | Seed                                | Used when                                    |
| -------- | ----------------------------------- | -------------------------------------------- |
| 1        | `ssn:{value}`                       | SSN present — strongest uniqueness guarantee |
| 2        | `email:{value}`                     | SSN missing, email present                   |
| 3        | `name_dob_zip:{name}\|{dob}\|{zip}` | Both SSN and email missing                   |
| 4        | `application:{id}\|row:{row_id}`    | All above missing — weakest                  |

Hash formula: `SHA-256(novacred_static_salt_v1 | seed)`

| Layer           | Direct PII              | Pseudonym                | Age Band      |
| --------------- | ----------------------- | ------------------------ | ------------- |
| Curated (audit) | ✅ Present (restricted) | —                        | —             |
| Analysis (safe) | ❌ Removed              | ✅ `applicant_pseudo_id` | ✅ `age_band` |

> **Production limitation:** The static salt means re-identification is possible if the salt is compromised. A production system should use a rotating salt stored in a key management service.

---

## 🕵️ GDPR Gap Analysis

8 gaps were identified between current data processing practices and the requirements of the [GDPR](https://gdpr-info.eu/):

| Severity    | Count |
| ----------- | ----- |
| 🔴 Critical | 3     |
| 🟠 High     | 4     |
| 🟡 Medium   | 1     |

### Full Gap Report

| Gap ID  | Gap                                                 | Status              | GDPR Article                                                                                                                                     | Severity    |
| ------- | --------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| GAP-001 | Missing consent timestamp                           | Absent from dataset | [Art. 6](https://gdpr-info.eu/art-6-gdpr/), [Art. 7](https://gdpr-info.eu/art-7-gdpr/)                                                           | 🔴 Critical |
| GAP-005 | No audit trail for automated decisions              | Absent from dataset | [Art. 22](https://gdpr-info.eu/art-22-gdpr/), [Art. 13](https://gdpr-info.eu/art-13-gdpr/)                                                       | 🔴 Critical |
| GAP-006 | SSN stored unencrypted                              | Absent from dataset | [Art. 25](https://gdpr-info.eu/art-25-gdpr/), [Art. 32](https://gdpr-info.eu/art-32-gdpr/)                                                       | 🔴 Critical |
| GAP-002 | Missing data retention policy                       | Absent from dataset | [Art. 5(1)(e)](https://gdpr-info.eu/art-5-gdpr/)                                                                                                 | 🟠 High     |
| GAP-004 | Missing processing purpose field                    | Absent from dataset | [Art. 5(1)(b)](https://gdpr-info.eu/art-5-gdpr/)                                                                                                 | 🟠 High     |
| GAP-003 | Missing data source / transparency field            | Absent from dataset | [Art. 14](https://gdpr-info.eu/art-14-gdpr/)                                                                                                     | 🟠 High     |
| GAP-007 | No human oversight documentation                    | Absent from dataset | [Art. 22](https://gdpr-info.eu/art-22-gdpr/), [AI Act Art. 14](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e2298-1-1) | 🟠 High     |
| GAP-008 | Sensitive behavioural data without explicit purpose | Absent from dataset | [Art. 5(1)(b)](https://gdpr-info.eu/art-5-gdpr/), [Art. 22](https://gdpr-info.eu/art-22-gdpr/)                                                   | 🟡 Medium   |

### Evidence: `algorithm_risk_score` as Dominant Rejection Reason (GAP-005)

81.6% of all rejections (169 out of 207) cite `algorithm_risk_score` as the sole reason. This directly evidences GAP-005: NovaCred is issuing consequential automated decisions with no meaningful explanation. Under [GDPR Art. 22](https://gdpr-info.eu/art-22-gdpr/), data subjects have the right to obtain an explanation and to contest the decision. Under [EU AI Act Art. 13](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e2238-1-1), the system must be sufficiently transparent that deployers can interpret its output. Neither requirement is met.

### Records of Processing Activities (ROPA — [Art. 30](https://gdpr-info.eu/art-30-gdpr/))

| Field                  | Value                                                                                             |
| ---------------------- | ------------------------------------------------------------------------------------------------- |
| Processing activity    | Credit application assessment                                                                     |
| Controller             | NovaCred                                                                                          |
| Purpose                | Automated creditworthiness assessment for loan approval decisions                                 |
| Legal basis            | Art. 6(1)(b) — contract performance **(UNCONFIRMED: `consent_timestamp` absent — GAP-001)**       |
| Data categories        | Name, email, SSN, IP address, DOB, gender, ZIP, financials, spending behaviour                    |
| Special categories     | None directly — ZIP may act as ethnic proxy ([Art. 9](https://gdpr-info.eu/art-9-gdpr/) risk)     |
| Recipients             | Internal credit scoring model; no third-party recipients documented                               |
| Retention period       | **NOT DEFINED — `retention_until` absent (GAP-002)**                                              |
| Security measures      | Pseudonymisation implemented; SSN encryption pending (GAP-006)                                    |
| DPIA required          | Yes — automated profiling with significant effects ([Art. 35](https://gdpr-info.eu/art-35-gdpr/)) |
| DPIA completed         | **No evidence found**                                                                             |
| EU AI Act registration | Required — HIGH-RISK system; **not yet completed**                                                |

### Breach Exposure Assessment ([Art. 33](https://gdpr-info.eu/art-33-gdpr/) / [Art. 34](https://gdpr-info.eu/art-34-gdpr/))

Given SSNs are stored in plain text (GAP-006), a breach today would trigger dual notification obligations:

| Metric                       | Count | Obligation                                                                                  | Notification Type                                     |
| ---------------------------- | ----- | ------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| Unique data subjects at risk | 502   | [Art. 33](https://gdpr-info.eu/art-33-gdpr/) + [Art. 34](https://gdpr-info.eu/art-34-gdpr/) | Supervisory authority (72h) + individual notification |
| SSNs exposed in plain text   | ~490+ | [Art. 34](https://gdpr-info.eu/art-34-gdpr/)                                                | **High-risk — individual notification required**      |
| Email addresses exposed      | ~490+ | [Art. 33](https://gdpr-info.eu/art-33-gdpr/)                                                | Supervisory authority notification                    |
| IP addresses exposed         | ~490+ | [Art. 33](https://gdpr-info.eu/art-33-gdpr/)                                                | Supervisory authority notification                    |
| Full names exposed           | ~490+ | [Art. 33](https://gdpr-info.eu/art-33-gdpr/)                                                | Supervisory authority notification                    |

Implementing **GOV-003** (SSN encryption + pseudonymisation) would significantly reduce Art. 34 exposure — pseudonymised data carries lower notification risk under [GDPR recital 26](https://gdpr-info.eu/recitals/no-26/).

### ZIP Code as Geographic Proxy ([Art. 9](https://gdpr-info.eu/art-9-gdpr/) Risk)

ZIP codes correlate with ethnic composition in many jurisdictions due to historical residential segregation. If ZIP is used as a model feature, NovaCred may be processing a proxy for ethnicity — a special category under [GDPR Art. 9](https://gdpr-info.eu/art-9-gdpr/) — without the heightened safeguards Art. 9 requires. GOV-010 addresses this risk.

---

## 🤖 EU AI Act Classification

NovaCred's credit scoring system is classified **HIGH-RISK** under [EU AI Act Annex III, Point 5(b)](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e3920-1-1):

> _"AI systems intended to be used for creditworthiness assessment or credit scoring of natural persons."_

NovaCred satisfies all three conditions: it is an ML system, it assesses creditworthiness, and it acts on individual natural persons. The combination of [GDPR Art. 22](https://gdpr-info.eu/art-22-gdpr/) and EU AI Act high-risk obligations creates a **dual compliance requirement** — both regimes apply simultaneously.

### Obligations & Current Status

| Article                                                                                     | Obligation                                        | Current Status                         |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------- | -------------------------------------- |
| [Art. 9](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e1991-1-1)  | Risk management system throughout lifecycle       | ❌ No evidence                         |
| [Art. 10](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e2041-1-1) | Data governance — training/inference data quality | ⚠️ Partial — DI = 0.77 flags data bias |
| [Art. 13](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e2238-1-1) | Transparency — deployers can interpret outputs    | ❌ `algorithm_risk_score` is opaque    |
| [Art. 14](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e2298-1-1) | Human oversight — ability to intervene/override   | ❌ No oversight mechanism documented   |
| [Art. 26](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e3478-1-1) | Deployer obligations — monitor operation          | ❌ No monitoring documented            |
| [Art. 72](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e6487-1-1) | EU database registration before deployment        | ❌ Not completed                       |
| [GDPR Art. 35](https://gdpr-info.eu/art-35-gdpr/)                                           | DPIA required before processing                   | ❌ No evidence found                   |

---

## 🛡️ Governance Recommendations

10 prioritised controls derived from the GDPR gap analysis, AI Act classification, and bias findings:

| Priority | Control | Category                     | Action                                            | Legal Reference                                                                                                                                       | Effort | Responsible                |
| -------- | ------- | ---------------------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | -------------------------- |
| 1        | GOV-001 | Legal Compliance             | Implement consent capture and tracking            | [GDPR Art. 6](https://gdpr-info.eu/art-6-gdpr/), [Art. 7](https://gdpr-info.eu/art-7-gdpr/)                                                           | Medium | Engineering + Legal        |
| 2        | GOV-002 | Legal Compliance             | Define and enforce data retention policy          | [GDPR Art. 5(1)(e)](https://gdpr-info.eu/art-5-gdpr/), [Art. 30](https://gdpr-info.eu/art-30-gdpr/)                                                   | Medium | Data Engineering + DPO     |
| 3        | GOV-003 | Security                     | Encrypt and pseudonymise SSNs                     | [GDPR Art. 25](https://gdpr-info.eu/art-25-gdpr/), [Art. 32](https://gdpr-info.eu/art-32-gdpr/)                                                       | Low    | Data Engineering           |
| 4        | GOV-004 | AI Act / Automated Decisions | Implement human oversight mechanism               | [GDPR Art. 22](https://gdpr-info.eu/art-22-gdpr/), [AI Act Art. 14](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e2298-1-1) | High   | Product + Operations       |
| 5        | GOV-005 | AI Act / Automated Decisions | Create decision audit trail                       | [GDPR Art. 22](https://gdpr-info.eu/art-22-gdpr/), [AI Act Art. 13](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e2238-1-1) | Medium | Data Science + Engineering |
| 6        | GOV-006 | Fairness                     | Address gender disparate impact (DI = 0.77)       | [AI Act Art. 10](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e2041-1-1), [GDPR Art. 22](https://gdpr-info.eu/art-22-gdpr/) | High   | Data Science + Legal       |
| 7        | GOV-007 | Transparency                 | Add data source and processing purpose fields     | [GDPR Art. 5(1)(b)](https://gdpr-info.eu/art-5-gdpr/), [Art. 14](https://gdpr-info.eu/art-14-gdpr/)                                                   | Low    | Engineering + Legal        |
| 8        | GOV-008 | AI Act                       | Conduct DPIA and register with EU AI Act database | [GDPR Art. 35](https://gdpr-info.eu/art-35-gdpr/), [AI Act Art. 72](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e6487-1-1) | High   | DPO + Legal + Management   |
| 9        | GOV-009 | Data Quality                 | Standardise gender encoding at source             | [GDPR Art. 5(1)(d)](https://gdpr-info.eu/art-5-gdpr/)                                                                                                 | Low    | Data Engineering           |
| 10       | GOV-010 | Privacy by Design            | Apply data minimisation to spending and ZIP data  | [GDPR Art. 5(1)(c)](https://gdpr-info.eu/art-5-gdpr/), [Art. 25](https://gdpr-info.eu/art-25-gdpr/)                                                   | Medium | Data Science + DPO         |

### Top 3 Immediate Actions

| Priority | Control                                 | Why Urgent                                                                                                                                                                          |
| -------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 🔴 1     | GOV-001 — Implement consent tracking    | Without `consent_timestamp`, NovaCred cannot prove lawful basis — **all current processing is at legal risk**                                                                       |
| 🔴 2     | GOV-003 — Encrypt & pseudonymise SSNs   | A breach today triggers Art. 34 individual notification for 500+ subjects; pipeline pseudonymisation is already implemented                                                         |
| 🔴 3     | GOV-004 — Add human oversight mechanism | Required simultaneously by [GDPR Art. 22](https://gdpr-info.eu/art-22-gdpr/) and [AI Act Art. 14](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e2298-1-1) |

---

## 📊 Governance Audit Summary

| Category            | Metric                                   | Value                       |
| ------------------- | ---------------------------------------- | --------------------------- |
| GDPR                | Total gaps identified                    | 8                           |
| GDPR                | Critical severity gaps                   | 3                           |
| GDPR                | High severity gaps                       | 4                           |
| PII                 | Total fields catalogued                  | 9                           |
| PII                 | Direct PII fields                        | 4                           |
| PII                 | Quasi-PII fields                         | 3                           |
| Bias                | Gender DI ratio                          | 0.77 (🔴 below 0.80)        |
| Bias                | Age-based DI                             | ✅ All bands above 0.80     |
| Automated Decisions | Rejections citing `algorithm_risk_score` | 169 (81.6%)                 |
| AI Act              | Risk classification                      | HIGH-RISK (Annex III §5(b)) |
| AI Act              | Obligations triggered                    | 7                           |
| AI Act              | Immediate compliance gaps                | 5                           |
| Recommendations     | Total governance controls proposed       | 10                          |

---

## ▶️ How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run notebooks in order (01 produces the CSVs that 02 and 03 consume)
python -m jupyter nbconvert --to notebook --execute notebooks/01-data-quality.ipynb --inplace
python -m jupyter nbconvert --to notebook --execute notebooks/02-bias-analysis.ipynb --inplace
python -m jupyter nbconvert --to notebook --execute notebooks/03-privacy-demo.ipynb --inplace
```

---

## 🎥 Presentation

Video available at:

---

## 📚 Legal References

| Regulation                                 | Link                                                                                          |
| ------------------------------------------ | --------------------------------------------------------------------------------------------- |
| GDPR full text                             | [gdpr-info.eu](https://gdpr-info.eu/)                                                         |
| EU AI Act full text                        | [EUR-Lex](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689)               |
| EU AI Act Annex III — High-Risk AI Systems | [Annex III](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689#d1e3920-1-1) |
| GDPR Art. 5 — Principles                   | [Art. 5](https://gdpr-info.eu/art-5-gdpr/)                                                    |
| GDPR Art. 6 — Lawful basis                 | [Art. 6](https://gdpr-info.eu/art-6-gdpr/)                                                    |
| GDPR Art. 22 — Automated decisions         | [Art. 22](https://gdpr-info.eu/art-22-gdpr/)                                                  |
| GDPR Art. 25 — Privacy by design           | [Art. 25](https://gdpr-info.eu/art-25-gdpr/)                                                  |
| GDPR Art. 30 — ROPA                        | [Art. 30](https://gdpr-info.eu/art-30-gdpr/)                                                  |
| GDPR Art. 35 — DPIA                        | [Art. 35](https://gdpr-info.eu/art-35-gdpr/)                                                  |
