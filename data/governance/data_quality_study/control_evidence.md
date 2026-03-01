## Data Contract / Control Evidence

### Objective

Demonstrate **audit-grade evidence** that our data quality controls (**policy-as-code rules + deterministic remediation**) materially improved the dataset **without breaking traceability**, and clearly document the **residual risks** that must be governed (rather than “silently fixed”).

---

### Evidence artifacts

- **Pre vs Post comparison (rule-level deltas):** `data/governance/data_quality_study/reports/evidence/pre_post_comparison.csv`
- **Selected residual issues (governance focus):** `data/governance/data_quality_study/reports/evidence/residual_issues_selected.csv`
- **Reproducible notebook (builds the evidence tables):** `data/governance/data_quality_study/pre_post_merge.ipynb`

---

### Process

#### 1) Policy-as-code

Quality expectations are expressed as stable rule IDs (e.g., `R_APP_008`, `R_DUP_001`) and evaluated deterministically (pass/fail flags). This creates an auditable **data contract** that is consistent across **pre-clean** and **post-clean** stages.

#### 2) Deterministic remediation

We apply deterministic transformations where corrections are defensible (e.g., canonical formatting). Where values are invalid/impossible, we **nullify rather than invent**, and retain traceability through flags/metadata.

#### 3) Evidence reporting

We aggregate rule failures into **counts** and **% affected**, then compare **pre vs post** to quantify improvement and highlight remaining governance-significant risks.

---

## Results

### Top improvements

From `pre_post_comparison.csv`:

- **R_APP_008 (DOB non-ISO format):** 31.27% → 0.00% (Δ **-31.27 pp**)
- **R_APP_006 (Gender non-canonical):** 22.11% → 0.00% (Δ **-22.11 pp**)
- **R_APP_012 (Negative credit history months):** 0.40% → 0.00% (Δ **-0.40 pp**)
- **R_APP_013 (Negative savings balance):** 0.20% → 0.00% (Δ **-0.20 pp**)
- **R_APP_014 (DTI outside [0,1]):** 0.20% → 0.00% (Δ **-0.20 pp**)

**Conclusion:** The largest wins come from **standardisation** (DOB, gender) and **validity enforcement via nullification** (numeric impossibilities). This improves downstream modelling reliability and reduces risk of spurious signals.

---

## Residual issues (what remains and how we govern it)

We do **not** aim for “0 remaining issues at any cost”. Instead, residual issues are explicitly documented and mapped to controls (upstream fixes, downstream mitigations, monitoring).

| Rule ID   | Post % affected | Post count | Severity | Issue                                   | Category            | Risk                                                                           | Recommended control/action                                                                                             |
| --------- | --------------: | ---------: | -------- | --------------------------------------- | ------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| R_APP_001 |          87.65% |        440 | High     | Missing/blank processing timestamp      | Upstream fix        | Weak audit trail / provenance; reduces traceability                            | Make `processing_timestamp` mandatory at source; enforce ingest validation; implement event logging / lineage          |
| R_APP_009 |           7.77% |         39 | Medium   | Ambiguous DOB format NN/NN/YYYY         | Mitigate downstream | Incorrect age derivation may affect fairness slices                            | Preserve `dob_ambiguous_flag`; prefer `age_band`; restrict direct DOB usage; monitor impact in fairness reporting      |
| R_APP_002 |           1.59% |          8 | High     | Required applicant fields missing/blank | Mitigate (control)  | Missingness may correlate with protected attributes; reduces model reliability | Add “reject/hold” rules in ingestion; require completion upstream; keep missingness flags; document handling           |
| R_DUP_003 |           1.20% |          6 | High     | SSN repeats across records              | Mitigate (control)  | Identity duplication/fraud risk; distorts fairness/performance metrics         | Monitor repeated identifiers; investigate anomalies; log cases; ensure dedup does not disproportionately impact groups |
| R_APP_003 |           1.00% |          5 | High     | Both SSN and IP missing/blank           | Mitigate downstream | Weak identity linkage; lower confidence pseudonymous linkage                   | Ensure robust pseudo-id fallback; track `pseudo_id_source`; treat as higher-uncertainty records                        |
| R_DUP_001 |           0.80% |          4 | High     | Duplicated application_id rows          | Mitigate (control)  | Double-counting/leakage in modelling/metrics                                   | Deterministic canonical record selection; retain duplicate log; enforce one-row-per-application in analysis layer      |
| R_APP_004 |           1.39% |          7 | Medium   | Email missing/blank                     | Mitigate (control)  | Completeness gap and potential bias correlation                                | Upstream validation; standardize blanks→NA; keep missingness flag; document downstream treatment                       |
| R_APP_005 |           0.80% |          4 | Medium   | Email invalid format                    | Mitigate (control)  | Low data quality; breaks validation and identity signals                       | Stronger normalization/regex validation at ingest; exclude from linkage logic                                          |

---

## Compliance mapping

### GDPR relevance (data protection + accountability)

Our controls support GDPR principles by improving data accuracy, limiting error propagation, and documenting governance decisions:

- **Accuracy & quality controls:** Supports the principle that personal data should be accurate and kept up to date (**GDPR Art. 5(1)(d)**).  
  https://eur-lex.europa.eu/eli/reg/2016/679/oj
- **Privacy by design & by default:** Our governance approach (deterministic processing, minimisation in the analysis layer, and explicit risk flagging) aligns with **GDPR Art. 25**.  
  https://eur-lex.europa.eu/eli/reg/2016/679/oj
- **Security of processing:** Audit evidence and access separation align with **GDPR Art. 32**.  
  https://eur-lex.europa.eu/eli/reg/2016/679/oj

**Governance note:** `R_APP_001` (missing processing timestamps) is a governance gap because it undermines traceability—this is not only “data quality”; it affects demonstrability and accountability.

### EU AI Act relevance (credit scoring / creditworthiness = high-risk)

Systems used to evaluate creditworthiness or establish credit scores are listed as **high-risk** (Annex III).  
https://eur-lex.europa.eu/eli/reg/2024/1689/oj

This makes the following requirements directly relevant to our governance approach:

- **Data governance & data quality:** **Art. 10** (training/validation/testing data governance and quality).  
  https://eur-lex.europa.eu/eli/reg/2024/1689/oj
- **Record-keeping / logging:** **Art. 12** (automatic logging). The residual issue `R_APP_001` (missing timestamps) is an upstream risk to address to support strong operational record-keeping.  
  https://eur-lex.europa.eu/eli/reg/2024/1689/oj
- **Human oversight:** **Art. 14**. Residual issues like ambiguous DOB and duplicates should be governed via review/monitoring rather than silent correction.  
  https://eur-lex.europa.eu/eli/reg/2024/1689/oj
