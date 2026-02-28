# Privacy & Data Minimization Baseline (Governance Officer)

## 1. Purpose and scope

This document provides **audit-ready privacy evidence** for the NovaCred credit application pipeline. The goal is to:

1. identify and classify **direct PII** vs **quasi-identifiers**,
2. justify **data minimization** and **access separation** decisions, and
3. provide reproducible evidence that the **analysis/modeling dataset** contains **no direct PII** and no detectable “PII leakage”.

## 2. Authoritative privacy reference (PII register)

We use `data/quality/catalogs/pii_inventory.csv` as the **authoritative privacy register**. It records for each field:

- `field_path` (canonical raw JSON field path),
- `classification` (PII / Quasi-PII / Non-PII),
- `present_in` (expected presence across `raw`, `curated`, `analysis`).

We standardize this register into a review-friendly matrix:

- `data/quality/catalogs/governance/pii_fields_study/pii_presence_matrix.csv`

This register underpins our governance decisions on **GDPR data minimization** and **access control separation**.

## 3. Direct PII and quasi-identifiers

### 3.1 Direct PII (forbidden in the analysis layer)

Fields classified as **PII** are treated as **direct identifiers** and must not be present in the modeling dataset. Examples include:

- `applicant_info.full_name`
- `applicant_info.email`
- `applicant_info.ssn`
- `applicant_info.ip_address`
- `applicant_info.date_of_birth`

We export the complete list of direct PII field paths as a reusable control artifact:

- `data/quality/catalogs/governance/pii_fields_study/direct_pii_fields_list.txt`

### 3.2 Quasi-identifiers (allowed with governance controls)

Some attributes may remain in the analysis layer but require documentation and monitoring, e.g.:

- `clean_zip_code` (location proxy risk),
- `clean_gender` (protected attribute used for fairness evaluation),
- `application_id` (unique key).

## 4. GDPR mapping (high-level)

- **Data minimization** (GDPR Art. 5(1)(c)): direct identifiers are excluded from the analysis layer and kept only in restricted layers where strictly necessary.  
  https://gdpr-info.eu/art-5-gdpr/
- **Privacy by design/default** (GDPR Art. 25): the project implements a privacy-safe analytic extract for modeling and fairness analysis.  
  https://gdpr-info.eu/art-25-gdpr/
- **Security of processing** (GDPR Art. 32): removing direct identifiers and using pseudonymous linkage supports confidentiality and reduces exposure risk.  
   https://gdpr-info.eu/art-32-gdpr/
  (Official regulation text: https://eur-lex.europa.eu/eli/reg/2016/679/oj/eng)

## 5. Evidence: no direct PII in the analysis dataset (structural checks)

We validate `data/curated/applications_analysis.csv` using:

1. **Exact match check**: whether any analysis column name equals a direct PII field path.
2. **Leaf-name check**: detects potential renaming (e.g., `email` instead of `applicant_info.email`).

Result:

- direct PII columns found (exact match): `[]`
- direct PII columns found (leaf-name match): `[]`

This provides concrete evidence that direct identifiers are excluded from the modeling dataset.

## 6. Evidence: leakage scan (content-level checks on text columns)

Direct PII can still leak via free-text columns or accidental joins. To detect leakage:

- we scan all text (`object`) columns in `applications_analysis.csv`,
- we sample **300 rows** with `random_state=42` for reproducibility,
- we search for strict patterns:
  - email-like,
  - IPv4-like,
  - SSN-like (XXX-XX-XXXX).

Evidence artifacts:

- `data/quality/catalogs/governance/pii_fields_study/analysis_pii_leakage_scan_summary.csv`
- `data/quality/catalogs/governance/pii_fields_study/analysis_pii_leakage_by_column.csv`

Result (current run):

- sample_size_rows = 300
- columns_scanned (text columns) = 6
- email_like: 0 cell hits; 0 columns with hits
- ip_like: 0 cell hits; 0 columns with hits
- ssn_like: 0 cell hits; 0 columns with hits

`analysis_pii_leakage_by_column.csv` is empty because there were no hits in any column (expected outcome).

## 7. Access separation and governance controls

- `applications_analysis.csv` is the **approved dataset** for modeling and bias analysis (privacy-safe extract).
- `applications_curated_full.csv` is the **traceable audit layer** (raw + cleaned + flags + duplicate metadata) and may still contain direct PII; it should be treated as **restricted**.

Recommended controls:

- least-privilege access to audit layers containing PII,
- access logging for restricted datasets,
- automated checks in the pipeline: fail builds if direct PII fields appear in analysis.

## 8. Conclusion

By combining an authoritative PII register with structural and content-level checks, we provide reproducible evidence that the modeling dataset supports **GDPR-aligned minimization** and **privacy-by-design**, while preserving auditability through a restricted traceability layer.
