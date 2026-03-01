## Pseudonymization Design

### 1. Purpose and scope

This section documents the pseudonymization mechanism used to enable **deterministic linkage** for analysis while preventing the exposure of **direct identifiers** in the analysis layer.

Goals:

- generate a stable pseudonymous identifier (`applicant_pseudo_id`) for linkage,
- record provenance of the seed strategy (`pseudo_id_source` + fallback flag),
- demonstrate (with evidence) that **direct PII is removed before** the analysis dataset is produced,
- provide governance guidance for **salt management**, access separation, and residual re-identification risk.

---

### 2. Implementation overview (how `applicant_pseudo_id` is generated)

`applicant_pseudo_id` is a deterministic salted hash:

- Hash: `SHA-256(salt | seed)`
- Seed strategy (priority order):
  1. SSN (preferred)
  2. Email (fallback)
  3. Name + DOB + ZIP (fallback)
  4. Application ID + row ID (last-resort fallback)

**Important minimization note:** direct identifiers (SSN/email/name/DOB/ZIP) are used **only upstream** to generate the pseudonym and are **not propagated** into the analysis/modeling dataset.

To support auditability, the pipeline outputs:

- `pseudo_id_source` (which seed path was used)
- `pseudo_id_fallback_used_flag` (`True` when the seed is not SSN)

**Evidence:**  
[Pseudonym assignment evidence](https://github.com/msusanrocha/DEGO_Project_Group03/blob/main/data/governance/pseudonymization_study/evidence_privacy_assign_pseudo_id.png)

---

### 3. Where the salt is stored

For the academic project, the salt is defined in code for reproducibility. **The salt value must not be exposed in documentation or in a public repository.** We therefore treat the salt as a secret and redact it in evidence artifacts.

- Example pattern (recommended): `HASH_SALT = os.getenv("HASH_SALT")`
- If a constant is used for coursework reproducibility: `HASH_SALT = "***REDACTED***"`

Governance note (production recommendation):

- In production, the salt should **never** be hardcoded in the repository; it should be stored in an environment variable or secret manager, **access-restricted**, and **versioned for rotation** (e.g., `salt_v1`, `salt_v2`), with strict change control.

---

### 4. Proof of minimization in the analysis build

#### 4.1 Drop direct PII columns

The build drops any direct PII columns listed in `config.DIRECT_PII_COLUMNS` before producing the analysis extract.

**Evidence (drop direct PII):**  
[Drop direct PII evidence](https://github.com/msusanrocha/DEGO_Project_Group03/blob/main/data/governance/pseudonymization_study/evidence_privacy_drop_direct_pii.png)

#### 4.2 Minimize the output schema

The build then selects only an approved list of analysis columns (`analysis_columns`) to reduce the analytical surface area and prevent audit/remediation fields leaking downstream.

**Evidence (analysis schema minimization):**  
[Analysis column minimization evidence](https://github.com/msusanrocha/DEGO_Project_Group03/blob/main/data/governance/pseudonymization_study/evidence_privacy_analysis_columns_minimization.png)

---

### 5. Results: monitoring metrics

We export monitoring evidence in:

- `Summary.txt`
- `pseudo_id_source_distribution.csv`
- `pseudonymization_metrics.csv`

Key results (from `Summary.txt` / metrics exports):

- Fallback used: **5 rows (1.0%)**
- Missing `applicant_pseudo_id`: **0**
- Duplicate `applicant_pseudo_id`: **2**
- Curated canonical rate: **99.6%**

Governance interpretation:

- **Fallback rate** is an operational signal of seed-field completeness (data quality) and should be monitored with thresholds.
- **Duplicate `applicant_pseudo_id` values** can be **expected** when the same applicant appears across multiple applications (intended linkage).
- **Unexpected risk to monitor:** collisions (extremely unlikely with SHA-256) or pipeline errors. If the duplicate rate increases materially, trigger investigation (e.g., check seed logic, salt versioning, and any upstream normalization issues).

---

### 6. GDPR mapping

#### GDPR

- **Pseudonymization definition** (GDPR Art. 4(5)): pseudonymization reduces direct identifiability but remains personal data if linkable with additional information.  
  https://gdpr-info.eu/art-4-gdpr/
- **Privacy by design/default** (GDPR Art. 25): pseudonymized linkage enables analysis while minimizing identifier exposure.  
  https://gdpr-info.eu/art-25-gdpr/
- **Security of processing** (GDPR Art. 32): pseudonymization supports confidentiality and reduces breach impact compared to storing direct identifiers in analysis workflows.  
  https://gdpr-info.eu/art-32-gdpr/
- **Accountability** (GDPR Art. 5(2)): evidence exports and code-level controls document the mechanism and its effectiveness.  
  https://gdpr-info.eu/art-5-gdpr/

(Official regulation text: https://eur-lex.europa.eu/eli/reg/2016/679/oj/eng)

#### EU AI Act (high-risk credit decisioning)

Credit scoring / creditworthiness assessment systems are listed as **high-risk** use cases under the **EU AI Act** (Annex III).  
https://eur-lex.europa.eu/eli/reg/2024/1689/oj/eng

**How the EU AI Act influences this chapter (pseudonymization):**

- **Data governance and data management expectations:** High-risk systems require stronger controls over how data is processed, accessed, and documented. Pseudonymization supports reducing exposure of direct identifiers while still enabling controlled linkage for auditing and monitoring.
- **Traceability and logging:** High-risk systems emphasize traceability. The fields `pseudo_id_source` and `pseudo_id_fallback_used_flag` provide auditable metadata about how identifiers were derived (while governance controls restrict access to this metadata to reduce attack surface).
- **Technical documentation and accountability:** High-risk obligations increase the importance of reproducible evidence (exports + notebooks) demonstrating that direct identifiers are excluded from analysis and that pseudonymous linkage is controlled.
- **Risk management and monitoring:** The Act reinforces ongoing monitoring of the pseudonymization process (fallback rate, source distribution, duplicate pseudo-id rate) as operational signals that the system’s data pipeline remains stable and controlled over time.

---

### 7. Governance controls and residual risk

**Residual risk**  
Pseudonymization is not anonymization. Re-identification risk increases if an attacker has:

- the salt, and
- access to the underlying PII used to form the seed (SSN/email/name/DOB/ZIP), or
- the ability to guess seeds plus access to the salt.

**Controls**

- **Salt management:** store salt outside the repository (secret manager / env var), restrict access, and enable rotation (versioned salt + change control).
- **Strict access separation:** audit datasets containing PII are restricted; analysis datasets are direct-PII-free.
- **Sensitive metadata handling:** treat `pseudo_id_source` and fallback flags as **sensitive metadata** (they reveal which identifiers are present) and restrict access accordingly.
- **Logging guardrails:** prevent printing raw identifiers in notebook outputs; export only aggregated metrics or redacted previews.
- **Monitoring:** track `pseudo_id_source` distribution, fallback rate, and duplicate pseudo-id rate; trigger review if metrics drift upward.

---

### 8. Conclusion

The pipeline provides deterministic pseudonymous linkage for analysis through a salted SHA-256 design, with explicit minimization controls (**PII drop + restricted output schema**) and monitoring metrics that support governance oversight and GDPR-aligned accountability. The design is governance-ready when the salt is treated as a secret (managed outside the repository) and when metadata such as `pseudo_id_source` is handled under least-privilege controls.
