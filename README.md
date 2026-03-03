# Current Status + todos
- @antonio working on notebook 3

- @antonio filling out standard structure in notebook 3

- @connor filling out standard structure in notebook 1

- @connor finalizing readme

- @madalena adding key insights to readme

- @antonio adding key insights + governance to readme

- @all finalising ppt with key insights, quantified + visualization until 07.03.


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

| Name | Role | GitHub | Contributions |
|------|------|--------|--------|
| Connor Brown | Data Engineer | @Connor144-iv | Data Quality Notebook + Output, Key Findings in Readme, Data Preparation for further analysis
| Madalena Rocha | Data Scientist | @msusanrocha | GitHub Setup, Bias Analysis Notebook + Output, Key Findings in Readme, Governance Support
| António Santos | Governance Officer | @antonioncmsantos-hue | Privacy Demo Notebook + Output, Key Findings in Readme, Governance Recommendations
| Nora Puchert | Product Lead | @npu99 | Readme, Structure tracking, Meeting organisation, Requirements guidance, Presentation

---

## 📌 Executive Summary

---

## 📁 Repository Structure

```
DEGO_PROJECT_GROUP03/
├── README.md                   
├── data/                       ← Dataset files
├── figures/ 
├── notebooks/
│   ├── 01-data-quality.ipynb   ← Data loading, cleaning, profiling
│   ├── 02-bias-analysis.ipynb  ← Fairness metrics, DI ratio, proxy analysis
│   └── 03-privacy-demo.ipynb   ← PII inventory, pseudonymization, GDPR mapping
└── presentation/               ← Video link
└── reports/                    ← Analysis outputs and documentation
├── src/
│   └── fairness_utils.py       ← 
```

---

## 🔍 Data Quality 

## Objectives
1. Identify and document all data quality issues in the raw NovaCred credit application data.
2. Quantify the extent of each issue.
3. Demonstrate remediation steps and their impact.
4. Provide analysis ready deliverables.

## Data Quality Definitions

| Issue Group        | Definition |
|--------------------|------------|
| Completeness       | Data required for business or regulatory purposes is missing, blank, or not populated. |
| Validity           | Data values violate defined business rules, formats, or allowable ranges. |
| Consistency        | Data is present but not stored in a standard, canonical, or expected representation. |
| Cross-field logic  | Data fields are individually valid but logically inconsistent when evaluated together. |
| Privacy            | Data contains sensitive attributes that require protection, masking, or governance control. |
| Uniqueness         | Records or identifiers that should be distinct appear more than once. |

| Rule ID        | Definition |
|---------------|------------|
| R_APP_001     | Missing or blank processing timestamp. |
| R_APP_002     | One or more required applicant fields missing or blank. |
| R_APP_003     | Both SSN and IP address are missing or blank. |
| R_APP_004     | Email is missing or blank. |
| R_APP_005     | Email does not match the expected format. |
| R_APP_006     | Gender is not already in canonical form. |
| R_APP_007     | Gender is outside the allowed values. |
| R_APP_008     | Date of birth is not in canonical YYYY-MM-DD form. |
| R_APP_009     | Date of birth matches an ambiguous NN/NN/YYYY pattern. |
| R_APP_010     | Annual income is stored as a string or cannot be coerced cleanly. |
| R_APP_011     | Annual salary is populated instead of annual income. |
| R_APP_012     | Credit history months is negative. |
| R_APP_013     | Savings balance is negative. |
| R_APP_014     | Debt-to-income is outside the allowed range [0, 1]. |
| R_APP_015     | Approved loan is missing interest_rate and/or approved_amount. |
| R_APP_016     | Rejected loan is missing rejection_reason. |
| R_APP_017     | Loan approved with zero months of credit history. |
| R_APP_018     | Loan approved with less than 6 months of credit history. |
| R_APP_019     | IP address is in a private range and likely masked or synthetic. |
| R_SPN_001     | Spending category is missing or blank. |
| R_SPN_002     | Spending amount cannot be parsed as numeric. |
| R_SPN_003     | Spending amount is negative. |
| R_DUP_001     | Rows with duplicated application_id values. |
| R_DUP_002     | Distinct application_id values that are duplicated. |
| R_DUP_003     | Rows where SSN repeats across one or more records. |
| R_DUP_004     | Distinct SSN values that appear across different application IDs. |
| R_DUP_CONFLICT| Duplicated application IDs classified as conflicts. |
| R_DUP_CANONICAL | Canonical application rows retained for downstream analysis. |
## Core Outputs

### `data/curated/`
- `applications_curated_full.csv`
  - Restricted audit dataset.
  - One row per raw record.
  - Contains raw columns, clean columns, a  remediation flag set, and duplicate metadata.
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

## Duplicate Handling Policy
Canonical application selection for downstream analysis follows this deterministic rule:

1. Parse `processing_timestamp`.
2. Keep the latest parsed timestamp within each `application_id`.
3. If timestamps are tied, missing, or unparseable, keep the row with the highest `application_row_id`.

## Cleaning Choices
- Raw values are never overwritten.
- Cleaned values are stored in `clean_*` columns.
- Only a small set of remediation flags is retained in `applications_curated_full.csv`.
- DOB ambiguity rule:
  - For `NN/NN/YYYY` where both parts are `<= 12`, parse as `MM/DD/YYYY` and set `dob_ambiguous_flag=True`.
- `annual_salary` is mapped into `clean_annual_income` only when `annual_income` is missing.
- Negative `credit_history_months`, out-of-range `debt_to_income`, and negative `savings_balance` are nullified in the clean columns.

## Privacy Policy
- `applications_curated_full.csv` is restricted and may contain direct PII.
- `applications_analysis.csv` is PII-safe.
  - Direct identifiers are removed.
  - Applicants are represented by `applicant_pseudo_id`.
  - Date of birth is replaced with `age_band`.

## How To Run
From the repo root:

```powershell
pip install -r requirements.txt
python -m jupyter nbconvert --to notebook --execute notebooks/01-data-quality.ipynb --inplace
```

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

---

## 🛡️ Governance Recommendations

---

## 🎥 Presentation

Video available at:

--- 


