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