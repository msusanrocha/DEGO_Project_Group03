from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CURATED_DIR = DATA_DIR / "curated"
QUALITY_DIR = DATA_DIR / "quality"

RAW_JSON_PATH = RAW_DIR / "raw_credit_applications.json"

APPLICATIONS_CURATED_FULL_PATH = CURATED_DIR / "applications_curated_full.csv"
APPLICATIONS_ANALYSIS_PATH = CURATED_DIR / "applications_analysis.csv"
SPENDING_ITEMS_CLEAN_PATH = CURATED_DIR / "spending_items_clean.csv"

DATA_QUALITY_REPORT_PATH = QUALITY_DIR / "data_quality_report.csv"
BEFORE_AFTER_COMPARISON_PATH = QUALITY_DIR / "before_after_comparison.csv"
DUPLICATE_ID_REPORT_PATH = QUALITY_DIR / "duplicate_id_report.csv"
RULE_CATALOG_PATH = QUALITY_DIR / "rule_catalog.csv"
PII_INVENTORY_PATH = QUALITY_DIR / "pii_inventory.csv"

# Project-level static salt for deterministic pseudonymisation.
HASH_SALT = "novacred_static_salt_v1"

ANALYSIS_REFERENCE_DATE = "2026-01-01"

DOB_AMBIGUITY_RULE = "When DOB is NN/NN/YYYY and both NN <= 12, parse as MM/DD/YYYY."

EMAIL_REGEX = r"^[^\s@]+@[^\s@]+\.[^\s@]+$"

GENDER_MAP = {
    "m": "Male",
    "male": "Male",
    "f": "Female",
    "female": "Female",
}

DIRECT_PII_COLUMNS = [
    "raw_applicant_full_name",
    "raw_applicant_email",
    "raw_applicant_ssn",
    "raw_applicant_ip_address",
    "raw_applicant_date_of_birth",
    "clean_email",
    "clean_date_of_birth",
]

REQUIRED_APPLICANT_RAW_COLUMNS = [
    "raw_applicant_full_name",
    "raw_applicant_email",
    "raw_applicant_ssn",
    "raw_applicant_ip_address",
    "raw_applicant_gender",
    "raw_applicant_date_of_birth",
    "raw_applicant_zip_code",
]
