# Identity Resolution Governance (Duplicates) — NovaCred Project

This folder documents the **identity resolution / duplicate-handling governance control** implemented for the NovaCred credit application dataset. The goal is to ensure that the dataset used for analysis/modeling is **canonical (one row per application)**, **auditable**, and that duplicate-handling choices do not silently introduce **fairness** or **accountability** risks.

---

## 1) What we are studying

In credit decision pipelines, duplicate records can arise from:

- repeated submissions,
- upstream ingestion issues,
- partial updates (“versioned” records),
- conflicting records for the same application identifier.

From a governance perspective, duplicates matter because they can:

- distort approval-rate statistics and fairness metrics (selection bias),
- reduce auditability (unclear provenance / “which record was used and why?”),
- lead to inconsistent decisions when conflicting fields exist (especially around identifiers and demographic/proxy attributes).

This work treats **identity resolution** as a **control mechanism**:

**detect → classify → deterministically resolve → log decisions → assess downstream risk**

---

## 2) Artifacts produced

All artifacts are stored in:

`data/governance/identity_resolution_policy/`

### Core evidence (machine-readable)

- `duplicate_conflicts_summary.csv`  
  Executive summary of duplicate counts, types, and canonical selection rule used.

- `duplicated_ids_evidence.csv`  
  The authoritative list of duplicated `application_id`s, their classification, and canonical selection metadata.

- `duplicate_group_check_gender.csv`  
  Governance check: duplicate/conflict rates by gender group.

- `duplicate_group_check_age_band.csv`  
  Governance check: duplicate/conflict rates by age_band group.

### Core evidence (human-readable)

- `duplicate_types_evidence_card.png`  
  A “fact card” summarising the duplicate situation and the key governance note.

### Reproducibility

- `identity_resolution_governance.ipynb`  
  Notebook that generates the evidence above from:
  - `data/quality/duplicate_id_report.csv`
  - `data/curated/applications_analysis.csv`

---

## 3) Process

### Step A — Detect & classify duplicates

We used `data/quality/duplicate_id_report.csv` as the identity resolution log, which provides:

- duplicated `application_id`,
- `classification` (`conflict` vs `versioned`),
- the selected canonical row and the selection reason,
- an “example differences” field highlighting which raw fields differed.

### Step B — Apply deterministic canonical selection (and record why)

The log shows the canonical selection rule used:

`missing_or_unparseable_timestamp_fallback_max_row_id`

This indicates the intended time-based selection could not be applied consistently because the required timestamp was missing or unparseable, so a deterministic fallback (max row_id) was used.

### Step C — Governance + fairness linkage check (minimum viable control)

We merged duplicate flags into `applications_analysis.csv` and produced governance checks:

- duplicate/conflict rate by `clean_gender`
- duplicate/conflict rate by `age_band`

This is **not** a statistical fairness test (N is very small here). It is a **process control**: because identity resolution choices can indirectly affect downstream fairness metrics, governance requires visibility into whether duplicates/conflicts concentrate in specific groups and whether missingness undermines fairness monitoring.

---

## 4) Results

### Duplicate inventory

- **Total duplicated application_ids:** 2
- **Conflict duplicates:** 1 (`app_001`)
- **Versioned duplicates:** 1 (`app_042`)

### What makes the conflict “governance-significant”

For `app_001`, the duplicates differ on sensitive fields including:  
`raw_applicant_ssn`, `raw_applicant_ip_address`, `raw_applicant_gender`, `raw_applicant_date_of_birth`, `raw_applicant_zip_code`

This is governance-significant because conflicting identity and demographic/proxy fields can influence downstream fairness metrics and auditability.

### Provenance/auditability signal

Canonical selection relied on fallback because the timestamp was missing/unparseable. This is a strong governance signal that **audit trail instrumentation is incomplete** and should be treated as an upstream control gap (not just a cleaning detail).

### Duplicate-group checks

From the generated checks:

- The **conflict duplicate** appears under **missing** categories for gender and age_band.
- With N=2 this is not evidence of discrimination, but it indicates:
  - missingness in key fields is intertwined with identity/provenance issues,
  - governance should require stronger validation and upstream capture of demographics/proxies used for fairness reporting (to avoid fairness “blind spots”).

---

## 5) Governance policy

### Deterministic canonicalization rule

1. **Primary rule (intended):** choose the record with latest valid `processing_timestamp`.
2. **Fallback rule (observed):** if timestamp is missing/unparseable, choose max `row_id`.

**Governance requirement:** every fallback must be logged as a provenance exception and reviewed for materiality.

### Conflict escalation

If `classification == "conflict"`:

- Do **not** silently “clean and move on”.
- Escalate to manual review with a written decision log:
  - what differed,
  - which record was chosen and why,
  - whether downstream metrics are impacted,
  - what upstream remediation is required.

### How this connects Governance Officer ↔ Data Scientist

- The **Governance Officer** ensures identity resolution is auditable (rules + logging + escalation) and monitors whether data controls may create uneven impacts across groups (**fairness-by-process**).
- The **Data Scientist** evaluates fairness on outcomes/models (**fairness-by-outcome**).
  These are complementary: fairness conclusions are only reliable if upstream controls (like deduplication) do not distort group slices.

---

## 6) GDPR mapping

This work supports GDPR principles and obligations:

- **Principles (lawfulness, fairness, transparency; purpose limitation; data minimisation; accuracy; integrity/confidentiality; accountability)**  
  Identity resolution reduces inconsistent “truths” and improves explainability of the dataset used for decisions/analysis.  
  GDPR Article 5: https://gdpr-info.eu/art-5-gdpr/

- **Security of processing (technical and organisational measures; pseudonymisation as a measure where appropriate)**  
  This governance approach assumes layered access (raw/curated vs analysis) and reinforces controlled handling of sensitive fields.  
  GDPR Article 32: https://gdpr-info.eu/art-32-gdpr/

- **DPIA relevance (contextual)**  
  Credit decisioning and profiling-like processing can trigger DPIA considerations. Governance documentation and evidence logs support DPIA workstreams.  
  GDPR Article 35: https://gdpr-info.eu/art-35-gdpr/

---

## 7) EU AI Act positioning

If NovaCred’s credit decisioning is implemented with an AI system used to **evaluate creditworthiness or establish credit scores**, it is typically treated as **high-risk** under **Annex III (access to essential private services)**.

AI Act (Regulation (EU) 2024/1689): https://eur-lex.europa.eu/eli/reg/2024/1689/oj/eng

The requirements most directly connected to this work include:

- **Risk management (Article 9)** — identity resolution and provenance gaps are lifecycle risks to manage.
- **Data governance (Article 10)** — data preparation must be appropriate, controlled, and documented.
- **Record-keeping/logging (Article 12)** — missing/unparseable timestamps highlight why logging is mandatory.
- **Transparency to deployers (Article 13)** — governance evidence supports transparency documentation.
- **Human oversight (Article 14)** — conflict duplicates require escalation and review.
- **Accuracy/robustness (Article 15)** — deterministic rules + exception tracking strengthen robustness.

(Each article is within the same official text linked above.)

---

## 8) How to reproduce

1. Open and run:
   - `data/governance/identity_resolution_policy/identity_resolution_governance.ipynb`

2. Ensure inputs exist:
   - `data/quality/duplicate_id_report.csv`
   - `data/curated/applications_analysis.csv`

3. Outputs will be regenerated in:
   - `data/governance/identity_resolution_policy/`

---

## 9) Limitations

- The number of duplicated IDs is very small (**N=2**), so the group checks are **governance controls**, not statistically meaningful bias findings.
- The observed fallback selection indicates a provenance gap: the correct response is **upstream instrumentation + required timestamps + logs**, not ad-hoc downstream fixes.
