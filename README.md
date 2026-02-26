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

- `catalogs/pii_inventory.csv`
  - Field-level privacy classification and dataset presence map.
- `catalogs/rule_catalog.csv` (additional)
  - Stage-aware source-of-truth metadata for all rules used in reports.
- `catalogs/data_dictionary.csv` (additional)
  - Canonical field-level dictionary across raw input and all output datasets.
- `catalogs/data_dictionary_business.csv` (additional)
  - Business-facing dictionary subset (meaning, expected type, privacy, usage).
- `catalogs/data_dictionary_lineage.csv` (additional)
  - Technical lineage-focused dictionary subset.