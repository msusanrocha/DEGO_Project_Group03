from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from . import schema

RULE_ANNOTATION_COLUMNS = [
    "rule_key",
    "rule_family",
    "field_path_annotated",
    "source_columns",
    "value_source",
    "dataset_scope",
    "denominator",
    "count_unit",
]


def _values_equal(left: Any, right: Any) -> bool:
    """Compare values with NA-aware equality semantics."""
    if pd.isna(left) and pd.isna(right):
        return True
    return left == right


def _rows_identical(group: pd.DataFrame, columns: list[str]) -> bool:
    """Return True if all rows in a group match for the selected columns."""
    if len(group) <= 1:
        return True
    baseline = group.iloc[0]
    for _, row in group.iloc[1:].iterrows():
        for col in columns:
            if not _values_equal(baseline[col], row[col]):
                return False
    return True


def _example_diff_columns(
    group: pd.DataFrame, canonical_row_id: int, compare_cols: list[str], max_cols: int = 6
) -> str:
    """Return a compact pipe-delimited list of differing columns vs canonical row."""
    canonical = group.loc[group["application_row_id"] == canonical_row_id]
    if canonical.empty:
        return ""
    canonical_row = canonical.iloc[0]
    for _, row in group.iterrows():
        if int(row["application_row_id"]) == canonical_row_id:
            continue
        diff_cols = [col for col in compare_cols if not _values_equal(row[col], canonical_row[col])]
        if diff_cols:
            return "|".join(diff_cols[:max_cols])
    return ""


def analyze_duplicate_ids(applications_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Classify duplicate application_id records and generate canonical selection metadata."""
    df = applications_df.copy()
    df["parsed_processing_timestamp"] = pd.to_datetime(
        df["raw_processing_timestamp"], errors="coerce", utc=True
    )

    duplicate_rows: list[dict[str, Any]] = []
    metadata_rows: list[dict[str, Any]] = []
    compare_cols_all = [c for c in df.columns if c not in {"application_row_id", "parsed_processing_timestamp"}]
    compare_cols_versioned = [
        c
        for c in compare_cols_all
        if c not in {"raw_processing_timestamp", "raw_notes"}
    ]

    grouped = df.groupby("application_id", dropna=False, sort=True)
    for application_id, group in grouped:
        group_sorted = group.sort_values("application_row_id").copy()
        row_ids = group_sorted["application_row_id"].astype(int).tolist()
        dup_count = len(group_sorted)
        is_duplicate = dup_count > 1

        parsed = group_sorted["parsed_processing_timestamp"]
        parsed_present = parsed.notna().any()

        if parsed_present:
            max_ts = parsed.max()
            ts_candidates = group_sorted.loc[parsed == max_ts]
            if len(ts_candidates) == 1:
                canonical_row_id = int(ts_candidates["application_row_id"].iloc[0])
                canonical_reason = "latest_processing_timestamp"
            else:
                canonical_row_id = int(ts_candidates["application_row_id"].max())
                canonical_reason = "timestamp_tie_fallback_max_row_id"
        else:
            canonical_row_id = int(group_sorted["application_row_id"].max())
            canonical_reason = "missing_or_unparseable_timestamp_fallback_max_row_id"

        if not is_duplicate:
            classification = "unique"
        elif _rows_identical(group_sorted, compare_cols_all):
            classification = "exact"
        elif _rows_identical(group_sorted, compare_cols_versioned):
            classification = "versioned"
        elif parsed_present and parsed.dropna().nunique() >= 1:
            classification = "versioned"
        else:
            classification = "conflict"

        if is_duplicate:
            duplicate_rows.append(
                {
                    "application_id": application_id,
                    "dup_count": dup_count,
                    "classification": classification,
                    "canonical_row_id": canonical_row_id,
                    "canonical_reason": canonical_reason,
                    "example_differences": _example_diff_columns(
                        group_sorted,
                        canonical_row_id=canonical_row_id,
                        compare_cols=compare_cols_all,
                    ),
                }
            )

        for rank, row_id in enumerate(row_ids, start=1):
            metadata_rows.append(
                {
                    "application_row_id": row_id,
                    "application_id": application_id,
                    "is_duplicate_id": bool(is_duplicate),
                    "dup_count": int(dup_count),
                    "rank_within_id": int(rank),
                    "is_canonical_for_analysis": bool(row_id == canonical_row_id),
                    "has_conflict": bool(classification == "conflict"),
                    "duplicate_classification": classification,
                    "canonical_reason": canonical_reason,
                }
            )

    duplicate_report = pd.DataFrame(duplicate_rows)
    if duplicate_report.empty:
        duplicate_report = pd.DataFrame(
            columns=[
                "application_id",
                "dup_count",
                "classification",
                "canonical_row_id",
                "canonical_reason",
                "example_differences",
            ]
        )
    else:
        duplicate_report = duplicate_report.sort_values("application_id").reset_index(drop=True)

    metadata_df = pd.DataFrame(metadata_rows).sort_values("application_row_id").reset_index(drop=True)
    return duplicate_report, metadata_df


def _example_ids(application_ids: pd.Series, mask: pd.Series, max_examples: int = 5) -> str:
    """Return pipe-delimited example application IDs for flagged records."""
    return "|".join(
        application_ids[mask]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .sort_values()
        .head(max_examples)
        .tolist()
    )


def _build_issue_row(
    *,
    stage: str,
    issue_type: str,
    field_path: str,
    rule_id: str,
    description: str,
    count: int,
    percent: float,
    severity: str,
    example_application_ids: str,
) -> dict[str, Any]:
    """Build a normalized quality-issue report row."""
    return {
        "stage": stage,
        "issue_type": issue_type,
        "field_path": field_path,
        "rule_id": rule_id,
        "description": description,
        "count": int(count),
        "percent": round(float(percent), 2),
        "severity": severity,
        "example_application_ids": example_application_ids,
    }


def assert_rule_catalog_coverage(
    report_df: pd.DataFrame,
    rule_catalog_df: pd.DataFrame,
    report_name: str,
    stage_aware: bool = True,
) -> None:
    """Fail fast if report rule IDs are not represented in the consolidated catalog."""
    if report_df.empty:
        return
    if stage_aware and "stage" in report_df.columns:
        missing_by_stage: dict[str, list[str]] = {}
        for stage in sorted(report_df["stage"].dropna().astype(str).unique()):
            report_ids = set(
                report_df.loc[report_df["stage"].astype(str) == stage, "rule_id"].dropna().astype(str)
            )
            catalog_ids = set(
                rule_catalog_df.loc[
                    rule_catalog_df["stage"].astype(str) == stage, "rule_id"
                ].dropna().astype(str)
            )
            missing = sorted(report_ids - catalog_ids)
            if missing:
                missing_by_stage[stage] = missing
        if missing_by_stage:
            details = "; ".join(
                f"{stage}: {', '.join(ids)}" for stage, ids in sorted(missing_by_stage.items())
            )
            raise ValueError(
                f"{report_name} contains rule_id values missing from rule catalog: {details}"
            )
        return

    report_ids = set(report_df["rule_id"].dropna().astype(str))
    catalog_ids = set(rule_catalog_df["rule_id"].dropna().astype(str))
    missing = sorted(report_ids - catalog_ids)
    if missing:
        raise ValueError(
            f"{report_name} contains rule_id values missing from rule catalog: {', '.join(missing)}"
        )


def annotate_report_with_rule_catalog(
    report_df: pd.DataFrame,
    rule_catalog_df: pd.DataFrame,
    report_name: str,
) -> pd.DataFrame:
    """Join consolidated rule annotations into a stage-aware report dataframe."""
    assert_rule_catalog_coverage(
        report_df=report_df,
        rule_catalog_df=rule_catalog_df,
        report_name=report_name,
        stage_aware=True,
    )
    catalog_subset = (
        rule_catalog_df[["stage", "rule_id", *RULE_ANNOTATION_COLUMNS]]
        .drop_duplicates(subset=["stage", "rule_id"])
        .copy()
    )
    annotated = report_df.merge(
        catalog_subset,
        on=["stage", "rule_id"],
        how="left",
        validate="many_to_one",
    )
    return annotated


def build_data_quality_report(
    *,
    applications_df: pd.DataFrame,
    application_flags: pd.DataFrame,
    duplicate_report: pd.DataFrame,
    duplicate_metadata: pd.DataFrame,
    spending_df: pd.DataFrame,
    spending_flags: pd.DataFrame,
    stage: str = "pre",
    application_rules: dict[str, schema.RuleDef] | None = None,
    spending_rules: dict[str, schema.RuleDef] | None = None,
    ssn_column: str = "raw_applicant_ssn",
    rule_catalog: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build an issue registry across application, spending, and uniqueness checks."""
    rows: list[dict[str, Any]] = []
    app_denominator = len(applications_df.index)
    spending_denominator = len(spending_df.index)
    app_rules = application_rules or schema.APPLICATION_RULES
    spend_rules = spending_rules or schema.SPENDING_RULES

    for flag_col, rule in app_rules.items():
        if flag_col not in application_flags.columns:
            continue
        mask = application_flags[flag_col].fillna(False).astype(bool)
        count = int(mask.sum())
        percent = (count / app_denominator) * 100 if app_denominator else 0.0
        rows.append(
            _build_issue_row(
                stage=stage,
                issue_type=rule.issue_type,
                field_path=rule.field_path,
                rule_id=rule.rule_id,
                description=rule.description,
                count=count,
                percent=percent,
                severity=rule.severity,
                example_application_ids=_example_ids(applications_df["application_id"], mask),
            )
        )

    if app_denominator:
        dup_mask = duplicate_metadata["is_duplicate_id"].fillna(False).astype(bool)
        rows.append(
            _build_issue_row(
                stage=stage,
                issue_type="Uniqueness",
                field_path="_id",
                rule_id="R_DUP_001",
                description="Rows with duplicated application_id values.",
                count=int(dup_mask.sum()),
                percent=(float(dup_mask.sum()) / app_denominator) * 100,
                severity="high",
                example_application_ids=_example_ids(
                    duplicate_metadata["application_id"],
                    dup_mask,
                ),
            )
        )
        rows.append(
            _build_issue_row(
                stage=stage,
                issue_type="Uniqueness",
                field_path="_id",
                rule_id="R_DUP_002",
                description="Distinct application_id keys that are duplicated.",
                count=int(len(duplicate_report.index)),
                percent=(float(len(duplicate_report.index)) / app_denominator) * 100,
                severity="high",
                example_application_ids="|".join(
                    duplicate_report["application_id"].astype(str).sort_values().head(5).tolist()
                ),
            )
        )

        if ssn_column in applications_df.columns:
            ssn = applications_df[ssn_column].fillna("").astype(str).str.strip()
        else:
            ssn = pd.Series([""] * app_denominator, index=applications_df.index, dtype="string")
        non_blank_ssn = ssn != ""
        ssn_counts = ssn[non_blank_ssn].value_counts()
        duplicated_ssn_values = ssn_counts[ssn_counts > 1].index
        dup_ssn_mask = ssn.isin(duplicated_ssn_values)
        rows.append(
            _build_issue_row(
                stage=stage,
                issue_type="Uniqueness",
                field_path="applicant_info.ssn",
                rule_id="R_DUP_003",
                description="Rows where SSN repeats across one or more records.",
                count=int(dup_ssn_mask.sum()),
                percent=(float(dup_ssn_mask.sum()) / app_denominator) * 100,
                severity="high",
                example_application_ids=_example_ids(applications_df["application_id"], dup_ssn_mask),
            )
        )

        ssn_to_app = (
            applications_df.loc[dup_ssn_mask, [ssn_column, "application_id"]]
            .dropna(subset=[ssn_column])
            .assign(**{ssn_column: lambda d: d[ssn_column].astype(str).str.strip()})
        )
        cross_app_ssn = (
            ssn_to_app.groupby(ssn_column)["application_id"].nunique().pipe(lambda s: s[s > 1])
        )
        cross_app_ssn_values = set(cross_app_ssn.index.tolist())
        cross_app_mask = (
            ssn.isin(cross_app_ssn_values)
        )
        rows.append(
            _build_issue_row(
                stage=stage,
                issue_type="Uniqueness",
                field_path="applicant_info.ssn",
                rule_id="R_DUP_004",
                description="Distinct SSN values that appear across different application IDs.",
                count=int(len(cross_app_ssn_values)),
                percent=(float(len(cross_app_ssn_values)) / app_denominator) * 100,
                severity="high",
                example_application_ids=_example_ids(applications_df["application_id"], cross_app_mask),
            )
        )

    for flag_col, rule in spend_rules.items():
        if flag_col not in spending_flags.columns:
            continue
        mask = spending_flags[flag_col].fillna(False).astype(bool)
        count = int(mask.sum())
        percent = (count / spending_denominator) * 100 if spending_denominator else 0.0
        rows.append(
            _build_issue_row(
                stage=stage,
                issue_type=rule.issue_type,
                field_path=rule.field_path,
                rule_id=rule.rule_id,
                description=rule.description,
                count=count,
                percent=percent,
                severity=rule.severity,
                example_application_ids=_example_ids(spending_df["application_id"], mask),
            )
        )

    report = pd.DataFrame(rows).sort_values(["stage", "rule_id"]).reset_index(drop=True)
    catalog_df = rule_catalog if rule_catalog is not None else schema.build_rule_catalog()
    return annotate_report_with_rule_catalog(
        report_df=report,
        rule_catalog_df=catalog_df,
        report_name="data_quality_report",
    )


def build_schema_validation_report(
    *,
    applications_df: pd.DataFrame,
    application_flags: pd.DataFrame,
    spending_df: pd.DataFrame,
    spending_flags: pd.DataFrame,
    stage: str = "pre",
    application_rules: dict[str, schema.RuleDef] | None = None,
    spending_rules: dict[str, schema.RuleDef] | None = None,
    rule_catalog: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate schema validation failures for applications and spending datasets."""
    app_rules = application_rules or schema.APPLICATION_RULES
    spend_rules = spending_rules or schema.SPENDING_RULES
    app_summary = schema.summarize_validation_flags(
        flags=application_flags,
        rules=app_rules,
        application_ids=applications_df["application_id"],
        stage=stage,
    )
    spending_summary = schema.summarize_validation_flags(
        flags=spending_flags,
        rules=spend_rules,
        application_ids=spending_df["application_id"],
        stage=stage,
    )
    report = pd.concat([app_summary, spending_summary], ignore_index=True).sort_values(
        ["stage", "rule_id"]
    )
    catalog_df = rule_catalog if rule_catalog is not None else schema.build_rule_catalog()
    return annotate_report_with_rule_catalog(
        report_df=report,
        rule_catalog_df=catalog_df,
        report_name="schema_validation_report",
    )


def summarise_cleaning_changes(clean_df: pd.DataFrame) -> pd.DataFrame:
    """Count records affected by key deterministic remediation actions."""
    actions = {
        "A_CLEAN_001": ("annual_income_from_salary_flag", "annual_salary mapped into clean_annual_income"),
        "A_CLEAN_002": ("credit_history_nullified_flag", "Negative credit_history_months nullified"),
        "A_CLEAN_003": ("dti_nullified_flag", "Out-of-range debt_to_income nullified"),
        "A_CLEAN_004": ("savings_nullified_flag", "Negative savings_balance nullified"),
        "A_CLEAN_005": ("dob_parse_failed_flag", "DOB parse failed and set to null"),
        "A_CLEAN_006": ("dob_ambiguous_flag", "DOB parsed using ambiguity fallback rule"),
    }
    rows = []
    denominator = len(clean_df.index)
    for action_id, (col, description) in actions.items():
        if col not in clean_df.columns:
            continue
        count = int(clean_df[col].fillna(False).astype(bool).sum())
        rows.append(
            {
                "action_id": action_id,
                "description": description,
                "count": count,
                "percent": round((count / denominator) * 100 if denominator else 0.0, 2),
            }
        )
    return pd.DataFrame(rows)


def _lookup_metric(report: pd.DataFrame, rule_id: str) -> tuple[int, float]:
    """Fetch count and percent for a given rule_id from a quality report."""
    row = report.loc[report["rule_id"] == rule_id]
    if row.empty:
        return 0, 0.0
    first = row.iloc[0]
    return int(first["count"]), float(first["percent"])


def build_before_after_comparison(
    *,
    pre_report: pd.DataFrame,
    post_report: pd.DataFrame,
    duplicate_report: pd.DataFrame,
    duplicate_metadata: pd.DataFrame,
    total_records: int,
    canonical_count: int,
) -> pd.DataFrame:
    """Create a compact pre-vs-post remediation comparison table."""
    metric_map = [
        ("Missing required applicant fields", "R_APP_002"),
        ("Missing processing timestamp", "R_APP_001"),
        ("Blank email", "R_APP_004"),
        ("Invalid email format", "R_APP_005"),
        ("Gender requires normalization", "R_APP_006"),
        ("DOB non-ISO format", "R_APP_008"),
        ("Annual income type/coercion issue", "R_APP_010"),
        ("Annual salary field drift", "R_APP_011"),
        ("Negative credit history months", "R_APP_012"),
        ("Negative savings balance", "R_APP_013"),
        ("Debt-to-income out of range", "R_APP_014"),
        ("Approved with credit history <6 months", "R_APP_018"),
        ("Spending missing category", "R_SPN_001"),
        ("Spending amount non-numeric", "R_SPN_002"),
        ("Spending amount negative", "R_SPN_003"),
    ]

    rows: list[dict[str, Any]] = []
    for metric, rule_id in metric_map:
        pre_count, pre_percent = _lookup_metric(pre_report, rule_id)
        post_count, post_percent = _lookup_metric(post_report, rule_id)
        rows.append(
            {
                "metric": metric,
                "rule_id": rule_id,
                "pre_count": pre_count,
                "pre_percent": round(pre_percent, 2),
                "post_count": post_count,
                "post_percent": round(post_percent, 2),
                "delta_count": post_count - pre_count,
                "delta_percent": round(post_percent - pre_percent, 2),
            }
        )

    duplicate_rows = int(duplicate_metadata["is_duplicate_id"].fillna(False).astype(bool).sum())
    conflict_ids = int(
        duplicate_report.loc[duplicate_report["classification"] == "conflict", "application_id"].nunique()
    )
    duplicate_percent = (duplicate_rows / total_records) * 100 if total_records else 0.0
    conflict_percent = (conflict_ids / total_records) * 100 if total_records else 0.0

    rows.extend(
        [
            {
                "metric": "Duplicate application_id rows",
                "rule_id": "R_DUP_001",
                "pre_count": duplicate_rows,
                "pre_percent": round(duplicate_percent, 2),
                "post_count": duplicate_rows,
                "post_percent": round(duplicate_percent, 2),
                "delta_count": 0,
                "delta_percent": 0.0,
            },
            {
                "metric": "Duplicate conflict IDs",
                "rule_id": "R_DUP_CONFLICT",
                "pre_count": conflict_ids,
                "pre_percent": round(conflict_percent, 2),
                "post_count": conflict_ids,
                "post_percent": round(conflict_percent, 2),
                "delta_count": 0,
                "delta_percent": 0.0,
            },
            {
                "metric": "Canonical rows selected for analysis",
                "rule_id": "R_DUP_CANONICAL",
                "pre_count": total_records,
                "pre_percent": 100.0 if total_records else 0.0,
                "post_count": canonical_count,
                "post_percent": round((canonical_count / total_records) * 100 if total_records else 0.0, 2),
                "delta_count": canonical_count - total_records,
                "delta_percent": round(
                    ((canonical_count / total_records) * 100 - 100.0) if total_records else 0.0,
                    2,
                ),
            },
        ]
    )

    return pd.DataFrame(rows)
