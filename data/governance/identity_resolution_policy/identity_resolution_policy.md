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

This work treats **identity resolution** as a **control mechanism**: detect → classify → deterministically resolve → log decisions → assess downstream risk.

---

## 2) Artifacts produced

All artifacts are stored in:

`data/governance/identity_resolution_policy/`

### Core evidence

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
  A “fact card” summarising the duplicate situation and key governance note.

### Reproducibility

- `identity_resolution_governance.ipynb`  
  Notebook that generates the above evidence from:
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

- duplicate rate by `clean_gender`
- duplicate rate by `age_band`

This is not a statistical fairness test (N is very small here), but a **governance safeguard** to ensure identity resolution does not disproportionately affect certain groups, and to flag upstream data quality issues (e.g., missing demographics) that can compromise fairness monitoring.

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
- With N=2, this is not evidence of bias by itself, but it _does_ indicate:
  - missingness in key fields is intertwined with auditability/identity issues,
  - governance should require stronger validation for demographic/proxy fields used in fairness reporting.

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

### Fairness safeguard

Even when fairness analysis is handled elsewhere, governance requires at minimum:

- monitoring whether duplicates/conflicts concentrate in specific groups (gender/age bands or proxy variables),
- documenting implications and mitigations,
- ensuring missingness is addressed to avoid “fairness blind spots”.

---

## 6) GDPR mapping

This work supports several GDPR principles and obligations:

- **Data minimisation & purpose limitation:** identity resolution is implemented on curated/analysis layers without requiring direct PII exposure beyond what is necessary for audit review. :contentReference[oaicite:0]{index=0}
- **Accuracy:** conflicting duplicates are an accuracy risk (two “truths” for one application). Detecting and resolving them, with escalation for conflicts, supports the accuracy principle. :contentReference[oaicite:1]{index=1}
- **Integrity & confidentiality + security of processing:** the approach assumes layered access (raw/curated vs analysis), and encourages pseudonymisation and controlled access to sensitive fields. :contentReference[oaicite:2]{index=2}
- **Accountability (demonstrability):** the existence of an identity resolution log plus reproducible evidence artifacts strengthens accountability (you can show what happened and why). :contentReference[oaicite:3]{index=3}
- **DPIA relevance (contextual):** credit decisioning and profiling-like processing can trigger DPIA considerations; at minimum, governance documentation should support DPIA workstreams. :contentReference[oaicite:4]{index=4}

---

## 7) EU AI Act positioning

If NovaCred’s credit decisioning is implemented with an AI system used to **evaluate creditworthiness or establish credit scores**, this use case is listed as **high-risk** under **Annex III (essential private services)**. :contentReference[oaicite:5]{index=5}

High-risk AI systems have requirements that align strongly with the governance gaps and controls identified here:

- **Risk management system (Article 9):** duplicate handling and missing timestamps are risks to fundamental rights and accountability that should be logged and managed over the lifecycle. :contentReference[oaicite:6]{index=6}
- **Data & data governance (Article 10):** systematic controls to ensure data quality, representativeness, and governance over data preparation align with identity resolution controls. :contentReference[oaicite:7]{index=7}
- **Record-keeping/logging (Article 12):** the missing/unparseable timestamp leading to fallback selection directly highlights why lifecycle logging is required. :contentReference[oaicite:8]{index=8}
- **Transparency to deployers (Article 13):** governance evidence provides the kind of information that should accompany a high-risk system’s operation and documentation. :contentReference[oaicite:9]{index=9}
- **Human oversight (Article 14):** conflict duplicates require escalation and human review. :contentReference[oaicite:10]{index=10}
- **Accuracy/robustness/cybersecurity (Article 15):** deterministic rules, exception tracking, and monitoring improve robustness of data inputs and outcomes. :contentReference[oaicite:11]{index=11}

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

- The number of duplicated IDs is very small (**N=2**), so the “group checks” are **governance controls**, not statistically meaningful bias findings.
- The observed fallback selection indicates a provenance gap: the correct response is **upstream instrumentation + required timestamps + logs**, not ad-hoc downstream fixes.
