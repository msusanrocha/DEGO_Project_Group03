# DEGO Project - Team 3
## Team Members
- Madalena Rocha
- Nora Puchert
- Connor Brown
- António Santos

## Project Description
Credit scoring bias analysis for DEGO course.


## NovaCred Data Engineering Pipeline

This repository implements an end-to-end post-ingestion pipeline for NovaCred credit application JSON data.

## What The Pipeline Does

1. Flattens nested JSON into analysis-friendly tables.
2. Detects and classifies duplicate `application_id` values before keying.
3. Defines executable schema/validation rules.
4. Profiles data quality issues with counts, percentages, and application ID examples.
5. Cleans and standardises fields with auditable `raw_*` vs `clean_*` columns and flags.
6. Produces privacy tagging output and PII inventory.
7. Pseudonymises applicants with salted SHA-256 and exports a PII-safe analysis dataset.

## Folder Outputs

### `data/curated/`

- `applications_curated_full.csv` (RESTRICTED / audit-only)
  - One row per raw record (not deduplicated).
  - Contains raw columns, clean columns, validation flags, and duplicate metadata.
- `applications_analysis.csv` (PII-safe analysis output)
  - One row per canonical application.
  - No direct PII columns (`full_name`, `email`, `ssn`, `ip_address`, raw/clean DOB string).
  - Includes `applicant_pseudo_id`, cleaned features, and key flags.
- `spending_items_clean.csv`
  - One row per spending item with cleaned category/amount and validation flags.

### `data/quality/`

- `reports/pre/data_quality_report.csv`
  - Issue registry with `issue_type`, `field_path`, `rule_id`, `description`, `count`, `percent`, `severity`, `example_application_ids`.
- `reports/post/data_quality_report_postclean.csv`
  - Same issue registry structure, computed on cleaned columns after remediation.
  - `duplicates/duplicate_id_report.csv`
  - Duplicate ID classification and canonical selection details
- `catalogs/pii_inventory.csv`
  - Field-level privacy classification and dataset presence map.
- `reports/pre/schema_validation_report.csv` (additional)
  - Aggregated failure counts/percentages per validation rule.
- `reports/post/schema_validation_report_postclean.csv` (additional)
  - Post-clean schema validation summary over cleaned fields.
- `reports/comparison/before_after_comparison.csv` (additional)
  - Compact pre-vs-post remediation evidence table with deltas.
- `catalogs/rule_catalog.csv` (additional)
  - Stage-aware source-of-truth metadata for all rules used in reports.
- `catalogs/data_dictionary.csv` (additional)
  - Canonical field-level dictionary across raw input and all output datasets.
- `catalogs/data_dictionary_business.csv` (additional)
  - Business-facing dictionary subset (meaning, expected type, privacy, usage).
- `catalogs/data_dictionary_lineage.csv` (additional)
  - Technical lineage-focused dictionary subset.

  ## Duplicate Handling Policy

Canonical record rule for analysis (`applications_analysis.csv`):

1. Parse `processing_timestamp` and select the latest record per `application_id`.
2. If timestamp is missing/unparseable or ties remain, select max `application_row_id`.

Classification for duplicate keys in `duplicate_id_report.csv`:

- `exact`: rows are identical.
- `versioned`: non-material/timestamp-style differences.
- `conflict`: material differences that need governance review.

## Cleaning and Standardisation Choices

- Raw values are never overwritten; clean values are written to `clean_*` columns.
- DOB parsing supports:
  - `YYYY-MM-DD`
  - `YYYY/MM/DD`
  - `DD/MM/YYYY` or `MM/DD/YYYY`
- DOB ambiguity rule:
  - For `NN/NN/YYYY` where both parts are `<= 12`, parse as `MM/DD/YYYY`, and set `dob_ambiguous_flag=True`.
- `annual_salary` field drift:
  - If `annual_income` is missing and `annual_salary` exists, map into `clean_annual_income` and set provenance flag.
- Out-of-range/nullification policy:
  - Negative `credit_history_months` -> flagged and nullified in clean column.
  - `debt_to_income` outside `[0, 1]` -> flagged and nullified in clean column.
  - Negative `savings_balance` -> flagged and nullified in clean column.

## PII Policy

- `applications_curated_full.csv`: **restricted** (audit-only), may contain direct PII.
- `applications_analysis.csv`: **PII-safe** output.
  - Direct identifiers removed.
  - Applicants represented via deterministic salted hash (`applicant_pseudo_id`).
  - Age represented as privacy-preserving `age_band` (not raw DOB).

Pseudonym seed fallback order:

1. SSN
2. Email (if SSN missing)
3. `full_name + date_of_birth + zip_code` (if SSN/email missing)
4. Deterministic application fallback

## How To Run

From repo root:

```powershell
pip install -r requirements.txt
python -m jupyter nbconvert --to notebook --execute notebooks/01-data-quality.ipynb --inplace
```

The notebook executes all stages and writes CSV artifacts to `data/curated/` and `data/quality/`.

## Code Layout

- `src/config.py`: paths, constants, salt, mappings.
- `src/io_utils.py`: JSON load + CSV output helpers.
- `src/flatten.py`: JSON-to-table transforms.
- `src/schema.py`: schema dictionary + executable validation rules.
- `src/quality.py`: duplicate analysis + quality and validation reporting.
- `src/clean.py`: deterministic cleaning and standardisation.
- `src/privacy.py`: redaction, pseudonymisation, PII inventory, analysis-safe dataset creation.
- `notebooks/01-data-quality.ipynb`: orchestration notebook.