## Pseudonymization Design

### 1. Purpose and scope

This section documents the pseudonymization mechanism used to enable deterministic linkage for analysis while preventing the exposure of direct identifiers in the analysis layer.

Goals:

- generate a stable pseudonymous identifier (`applicant_pseudo_id`) for linkage,
- record provenance of the seed strategy (`pseudo_id_source` + fallback flag),
- demonstrate (with evidence) that direct PII is removed before the analysis dataset is produced,
- provide governance guidance for salt management and residual re-identification risk.

### 2. Implementation overview (how `applicant_pseudo_id` is generated)

`applicant_pseudo_id` is a deterministic salted hash:

- Hash: `SHA-256(salt | seed)`
- Seed strategy (priority order):
  1. SSN (preferred)
  2. Email (fallback)
  3. Name + DOB + ZIP (fallback)
  4. Application ID + row ID (last-resort fallback)

To support auditability, the pipeline outputs:

- `pseudo_id_source` (which seed path was used)
- `pseudo_id_fallback_used_flag` (`True` when not SSN)

**Evidence (pseudonym assignment logic):**  
[Pseudonym assignment evidence](https://github.com/msusanrocha/DEGO_Project_Group03/blob/main/data/governance/pseudonymization_study/evidence_privacy_assign_pseudo_id.png)

### 3. Where the salt is stored (current design)

The salt is currently defined in code as a project-level constant:

- `HASH_SALT = "novacred_static_salt_v1"`

**Evidence (salt in config):**  
[HASH_SALT evidence](https://github.com/msusanrocha/DEGO_Project_Group03/blob/main/data/governance/pseudonymization_study/evidence_config_hash_salt.png)

Governance note (production recommendation):

- In production, the salt should not be hardcoded in the repository; it should be stored in an environment variable or secret manager, access-restricted and versioned for rotation.

### 4. Proof of minimization in the analysis build

#### 4.1 Drop direct PII columns

The build drops any direct PII columns listed in `config.DIRECT_PII_COLUMNS` before producing the analysis extract.

**Evidence (drop direct PII):**  
[Drop direct PII evidence](https://github.com/msusanrocha/DEGO_Project_Group03/blob/main/data/governance/pseudonymization_study/evidence_privacy_drop_direct_pii.png)

#### 4.2 Minimize the output schema

The build then selects only an approved list of analysis columns (`analysis_columns`) to reduce the analytical surface area and prevent audit/remediation fields leaking downstream.

**Evidence (analysis schema minimization):**  
[Analysis column minimization evidence](https://github.com/msusanrocha/DEGO_Project_Group03/blob/main/data/governance/pseudonymization_study/evidence_privacy_analysis_columns_minimization.png)

### 5. Results: monitoring metrics (current run)

We export monitoring evidence in:

- `data/governance/pseudonymization_study/Summary.txt`
- `data/governance/pseudonymization_study/pseudo_id_source_distribution.csv`
- `data/governance/pseudonymization_study/pseudonymization_metrics.csv`

Key results (from `Summary.txt` / metrics exports):

- Fallback used: **5 rows (1.0%)**
- Missing `applicant_pseudo_id`: **0**
- Duplicate `applicant_pseudo_id`: **2**
- Curated canonical rate: **99.6%**

Governance interpretation:

- Fallback rate provides an operational signal of seed-field completeness (data quality).
- Duplicates may be expected if the same applicant appears across multiple applications; nonetheless, duplicates should be monitored to detect unintended collisions.

### 6. GDPR mapping (pseudonymization-specific)

- Pseudonymization definition (GDPR Art. 4(5)): pseudonymization reduces direct identifiability but remains personal data if linkable with additional information.  
  https://gdpr-info.eu/art-4-gdpr/

- Privacy by design/default (GDPR Art. 25): pseudonymized linkage enables analysis while minimizing identifier exposure.  
  https://gdpr-info.eu/art-25-gdpr/

- Security of processing (GDPR Art. 32): pseudonymization supports confidentiality and reduces breach impact compared to storing direct identifiers in analysis workflows.  
  https://gdpr-info.eu/art-32-gdpr/

- Accountability (GDPR Art. 5(2)): evidence exports and code-level controls document the mechanism and its effectiveness.  
  https://gdpr-info.eu/art-5-gdpr/

(Official regulation text: https://eur-lex.europa.eu/eli/reg/2016/679/oj/eng)

### 7. Governance controls and residual risk

**Residual risk**
Pseudonymization is not anonymization. Re-identification risk increases if an attacker has:

- the salt, and
- access to the underlying PII used to form the seed (SSN/email/name/DOB/ZIP), or
- the ability to guess seeds plus access to the salt.

**Controls**

- Salt stored outside the repository (secret manager / env var), access restricted, rotateable (versioned).
- Strict access separation: audit datasets containing PII are restricted; analysis datasets are PII-free.
- Logging guardrails: prevent printing raw identifiers in notebook outputs.
- Monitoring: track `pseudo_id_source` distribution and fallback rate, trigger review if fallback increases.

### 8. Conclusion

The pipeline provides deterministic pseudonymous linkage for analysis through a salted SHA-256 design, with explicit minimization controls (PII drop + restricted output schema) and monitoring metrics that support governance oversight and GDPR-aligned accountability.
