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