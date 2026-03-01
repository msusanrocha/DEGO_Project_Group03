# Privacy, Data Minimization, and AI Governance Baseline

## 1. Purpose and scope

This document provides **audit-ready privacy and governance evidence** for the NovaCred credit application pipeline. The objectives are to:

1. identify and classify **direct PII** vs **quasi-identifiers**,
2. justify **data minimization** and **access separation** decisions across layers (raw / curated / analysis),
3. provide reproducible evidence that the **analysis/modeling dataset contains no direct PII** and no detectable “PII leakage”, and
4. position the pipeline under **GDPR** and the **EU AI Act** obligations applicable to credit decisioning.

---

## 2. Authoritative privacy reference

We use **`pii_inventory.csv`** as the **authoritative privacy register**. It records, for each field:

- `field_path` (canonical raw JSON field path),
- `classification` (**PII / Quasi-PII / Non-PII**),
- expected presence across layers (`present_in_raw`, `present_in_curated`, `present_in_analysis`).

From this register, we also export:

- a review-friendly **presence matrix** (`pii_presence_matrix.csv`), and
- a reusable direct-PII list (`direct_pii_fields_list.txt`) used as a governance control.

### PII presence snapshot

The table below (excerpt) shows the key privacy expectation that **direct identifiers may exist in raw/curated but must not appear in analysis**, while certain quasi-identifiers are allowed with controls:

| field_path                   | classification | present_in_raw | present_in_curated | present_in_analysis |
| ---------------------------- | -------------- | -------------- | ------------------ | ------------------- |
| age_band                     | Non-PII        | False          | False              | True                |
| applicant_info.date_of_birth | PII            | True           | True               | False               |
| applicant_info.email         | PII            | True           | True               | False               |
| applicant_info.full_name     | PII            | True           | True               | False               |
| applicant_info.ip_address    | PII            | True           | True               | False               |
| applicant_info.ssn           | PII            | True           | True               | False               |
| applicant_info.gender        | Quasi-PII      | True           | True               | True                |
| applicant_info.zip_code      | Quasi-PII      | True           | True               | True                |
| applicant_pseudo_id          | Quasi-PII      | False          | False              | True                |
| application_id               | Quasi-PII      | True           | True               | True                |

This register underpins governance decisions on **GDPR minimization** and **access control separation**.

---

## 3. Direct PII vs quasi-identifiers

### 3.1 Direct PII

Fields classified as **PII** are treated as **direct identifiers** and must not be present in the modeling dataset. Examples include:

- `applicant_info.full_name`
- `applicant_info.email`
- `applicant_info.ssn`
- `applicant_info.ip_address`
- `applicant_info.date_of_birth`

**Control artifact:** we export the full set of direct PII field paths as `direct_pii_fields_list.txt` to support automated checks (e.g., fail the pipeline if any direct PII appears in the analysis layer).

### 3.2 Quasi-identifiers (allowed with governance controls)

Some attributes may remain in the analysis layer but require explicit governance, documentation, and monitoring (proxy and linkage risk), including:

- `applicant_info.zip_code` (location proxy risk),
- `applicant_info.gender` (protected attribute used for fairness evaluation),
- `age_band` (privacy-preserving alternative to raw date of birth),
- `application_id` (internal unique identifier; linkage risk if exported broadly),
- `applicant_pseudo_id` (pseudonymous linkage key; must be governed like sensitive metadata).

---

## 4. GDPR mapping (high-level)

Key GDPR principles supported by this design include:

- **Data minimization** (Art. 5(1)(c)): direct identifiers are excluded from the analysis layer and kept only where strictly necessary (e.g., restricted audit layers).  
  https://gdpr-info.eu/art-5-gdpr/
- **Storage limitation** (Art. 5(1)(e)): analysis extracts avoid replicating identifiers across downstream workflows.  
  https://gdpr-info.eu/art-5-gdpr/
- **Accountability** (Art. 5(2)): exported evidence artifacts + a reproducible notebook provide verifiable documentation of minimization controls.  
  https://gdpr-info.eu/art-5-gdpr/
- **Privacy by design/default** (Art. 25): the pipeline produces a privacy-safe analytical extract by default (model-safe dataset).  
  https://gdpr-info.eu/art-25-gdpr/
- **Security of processing** (Art. 32): removing direct identifiers and using pseudonymous linkage reduces exposure risk and supports confidentiality.  
  https://gdpr-info.eu/art-32-gdpr/  
  (Official regulation text: https://eur-lex.europa.eu/eli/reg/2016/679/oj/eng)

---

## 5. EU AI Act positioning

NovaCred uses AI for credit decisioning / lending outcomes. Under the **EU AI Act**, AI systems intended to evaluate the **creditworthiness of natural persons** or establish their credit score are listed as **high-risk** use cases in **Annex III**.

https://eur-lex.europa.eu/eli/reg/2024/1689/oj/eng

**Governance implication:** high-risk positioning strengthens the need for:

- rigorous **data governance** (quality, representativeness, traceability),
- **logging / auditability** of decisions and pipeline steps,
- clear **technical documentation** and internal controls,
- **human oversight** procedures, and
- ongoing monitoring of **accuracy, robustness, and bias**.

(We operationalize the data-side of these requirements through minimization, access separation, audit-ready curated layers, and evidence artifacts generated from the pipeline.)

---

## 6. Evidence: no direct PII in the analysis dataset

We validate `applications_analysis.csv` using two complementary checks:

1. **Exact-match check**: whether any analysis column name equals a direct PII field path.
2. **Leaf-name check**: detects potential renaming (e.g., `email` instead of `applicant_info.email`).

**Result (current run):**

- direct PII columns found (exact match): `[]`
- direct PII columns found (leaf-name match): `[]`

This provides concrete evidence that direct identifiers are excluded from the modeling dataset.

---

## 7. Evidence: leakage scan

Direct PII can still leak via free-text fields or accidental joins. To detect leakage:

- we scan **all rows** (full dataset) across all text (`object`) columns,
- we use **strict, low–false-positive patterns**:
  - email-like,
  - IPv4-like,
  - SSN-like with hyphens (XXX-XX-XXXX).

**Why strict patterns?** We avoid overly broad heuristics like “any 9-digit number” because pseudonymous identifiers (e.g., `applicant_pseudo_id`) can be numeric and would trigger false positives.

Evidence artifacts:

- `analysis_pii_leakage_scan_summary.csv` (pattern-level counts)
- `analysis_pii_leakage_by_column.csv` (column-level hits; expected to be empty)

**Expected outcome:** 0 hits for strict email/IP/SSN formats; an empty “by column” file.

---

## 8. Access separation and governance controls

- `applications_analysis.csv` is the **approved dataset** for modeling and bias analysis (privacy-safe extract). It includes `applicant_pseudo_id` for pseudonymous linkage and uses privacy-preserving features such as `age_band` rather than raw date of birth.
- `applications_curated_full.csv` is the **traceable audit layer** (raw + cleaned + flags + duplicate metadata) and may contain direct PII; it must be treated as **restricted** and not used for modeling.

Recommended controls (operational):

- **least-privilege** access to any layer that contains direct PII,
- **access logging** for restricted datasets,
- automated pipeline guardrail: **fail builds if direct PII fields appear in analysis** (using `direct_pii_fields_list.txt`),
- explicit governance for quasi-identifiers / proxies (ZIP, gender, spending-related features): documentation + periodic fairness monitoring.

**Public repository hygiene:** if the repository is public, datasets containing direct PII should not be committed; store only privacy-safe extracts and aggregated evidence.

---

## 9. Conclusion

Using an authoritative PII register plus structural and content-level checks, we provide reproducible evidence that the modeling dataset supports **GDPR-aligned minimization** and **privacy-by-design**, while maintaining auditability through a restricted traceability layer. The same controls strengthen readiness for the **EU AI Act high-risk** governance expectations in credit decisioning contexts.
