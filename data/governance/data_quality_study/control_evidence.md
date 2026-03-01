# Top 5 Improvements

- R_APP_008: 31.27% → 0.00% (Δ -31.27 pp)
- R_APP_006: 22.11% → 0.00% (Δ -22.11 pp)
- R_APP_012: 0.40% → 0.00% (Δ -0.40 pp)
- R_APP_013: 0.20% → 0.00% (Δ -0.20 pp)
- R_APP_014: 0.20% → 0.00% (Δ -0.20 pp)

## Residual Issues Table

Use these columns:

- Rule ID
- Post % affected
- Post count
- Severity
- Issue (description)
- Category
- Risk
- Recommended control/action

| Rule ID   | Post % affected | Post count | Severity | Issue (description)                                 | Category            | Risk                                                                                      | Recommended control/action                                                                                                                                  |
| --------- | --------------: | ---------: | -------- | --------------------------------------------------- | ------------------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| R_APP_001 |          87.65% |        440 | High     | Missing or blank processing timestamp               | Upstream fix        | Weak audit trail / provenance; reduces traceability and accountability                    | Make `processing_timestamp` mandatory at source; enforce ingest validation; implement event logging/lineage (ETL logs + record-level timestamps)            |
| R_APP_009 |           7.77% |         39 | Medium   | DOB format is ambiguous NN/NN/YYYY                  | Mitigate downstream | Incorrect age derivation may affect fairness slices and analysis                          | Preserve `dob_ambiguous_flag`; prefer `age_band` for analytics; restrict direct DOB usage; monitor impact in fairness reporting                             |
| R_APP_002 |           1.59% |          8 | High     | One or more required applicant fields missing/blank | Mitigate (control)  | Incomplete applicant profiles can bias decisions and reduce model reliability             | Add “reject/hold” rules in ingestion; require completion upstream; keep missingness flags; document handling policy                                         |
| R_DUP_003 |           1.20% |          6 | High     | Rows where SSN repeats across one or more records   | Mitigate (control)  | Identity duplication/fraud risk; can distort fairness and performance metrics             | Monitor repeated identifiers; investigate for fraud/data issues; ensure canonicalization does not disproportionately remove any group; log cases for review |
| R_APP_003 |           1.00% |          5 | High     | Both SSN and IP address missing/blank               | Mitigate downstream | Weak identity linkage; reduces reliability of pseudonymous linkage and deduplication      | Ensure robust pseudo-id fallback strategy; track `pseudo_id_source`; treat as higher-uncertainty records in analysis                                        |
| R_DUP_001 |           0.80% |          4 | High     | Rows with duplicated application_id values          | Mitigate (control)  | Double-counting; potential leakage in modeling/metrics                                    | Use deterministic canonical record selection (document rule); retain duplicate log; exclude duplicates from modeling layer (one row per application)        |
| R_APP_004 |           1.39% |          7 | Medium   | Email missing or blank                              | Mitigate (control)  | Data completeness gap; reduces contactability and may correlate with protected attributes | Enforce input validation upstream; standardize blanks→NA; keep missingness flag; document downstream treatment                                              |
| R_APP_005 |           0.80% |          4 | Medium   | Email does not match expected format                | Mitigate (control)  | Low data quality; can break downstream validation and dedup signals                       | Apply stricter normalization/regex validation at ingest; flag and exclude from any identity/linkage logic                                                   |
