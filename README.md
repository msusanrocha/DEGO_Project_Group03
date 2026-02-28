# DEGO Project - Team 3

## Project Objective
This pipeline is intentionally scoped to three objectives:

1. Identify and document all data quality issues in the raw NovaCred credit application data.
2. Quantify the extent of each issue.
3. Demonstrate remediation steps and their impact.

The deliverable keeps the two downstream analysis tables that later notebooks depend on, but removes wider governance artifacts that are not needed for those objectives.

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
