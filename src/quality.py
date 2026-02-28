from __future__ import annotations

from typing import Any

import pandas as pd

from . import schema


SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2}


def _values_equal(left: Any, right: Any) -> bool:
    """Compare scalar values with NA-aware equality semantics."""
    if pd.isna(left) and pd.isna(right):
        return True
    return left == right


def _rows_identical(group: pd.DataFrame, columns: list[str]) -> bool:
    """Return True when all rows in the group match across the selected columns."""
    if len(group) <= 1:
        return True
    baseline = group.iloc[0]
    for _, row in group.iloc[1:].iterrows():
        for column in columns:
            if not _values_equal(baseline[column], row[column]):
                return False
    return True


def _example_diff_columns(group: pd.DataFrame, canonical_row_id: int, compare_cols: list[str], max_cols: int = 5) -> str:
    """Return a compact pipe-delimited list of columns that differ from the canonical row."""
    canonical = group.loc[group["application_row_id"] == canonical_row_id]
    if canonical.empty:
        return ""
    canonical_row = canonical.iloc[0]
    for _, row in group.iterrows():
        if int(row["application_row_id"]) == canonical_row_id:
            continue
        diff_cols = [column for column in compare_cols if not _values_equal(row[column], canonical_row[column])]
        if diff_cols:
            return "|".join(diff_cols[:max_cols])
    return ""


def analyze_duplicate_ids(applications_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Classify duplicate application IDs and mark canonical rows for analysis."""
    df = applications_df.copy()
    df["parsed_processing_timestamp"] = pd.to_datetime(df["raw_processing_timestamp"], errors="coerce", utc=True)

    compare_cols_all = [column for column in df.columns if column not in {"application_row_id", "parsed_processing_timestamp"}]
    compare_cols_versioned = [column for column in compare_cols_all if column not in {"raw_processing_timestamp", "raw_notes"}]

    duplicate_rows: list[dict[str, Any]] = []
    metadata_rows: list[dict[str, Any]] = []

    for application_id, group in df.groupby("application_id", dropna=False, sort=True):
        group_sorted = group.sort_values("application_row_id").copy()
        dup_count = len(group_sorted)
        is_duplicate = dup_count > 1

        parsed = group_sorted["parsed_processing_timestamp"]
        if parsed.notna().any():
            latest_ts = parsed.max()
            candidates = group_sorted.loc[parsed == latest_ts]
            if len(candidates) == 1:
                canonical_row_id = int(candidates["application_row_id"].iloc[0])
                canonical_reason = "latest_processing_timestamp"
            else:
                canonical_row_id = int(candidates["application_row_id"].max())
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
        elif parsed.notna().any():
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
                    "example_differences": _example_diff_columns(group_sorted, canonical_row_id, compare_cols_all),
                }
            )

        for _, row in group_sorted.iterrows():
            metadata_rows.append(
                {
                    "application_row_id": int(row["application_row_id"]),
                    "application_id": application_id,
                    "is_duplicate_id": bool(is_duplicate),
                    "is_canonical_for_analysis": bool(int(row["application_row_id"]) == canonical_row_id),
                    "has_conflict": bool(classification == "conflict"),
                }
            )

    duplicate_report = pd.DataFrame(
        duplicate_rows,
        columns=["application_id", "dup_count", "classification", "canonical_row_id", "canonical_reason", "example_differences"],
    ).sort_values("application_id").reset_index(drop=True)

    metadata = pd.DataFrame(
        metadata_rows,
        columns=["application_row_id", "application_id", "is_duplicate_id", "is_canonical_for_analysis", "has_conflict"],
    ).sort_values("application_row_id").reset_index(drop=True)
    return duplicate_report, metadata


def _example_ids(application_ids: pd.Series, mask: pd.Series, max_examples: int = 5) -> str:
    """Return a compact pipe-delimited set of example application IDs for a flagged mask."""
    return "|".join(
        application_ids[mask].dropna().astype(str).drop_duplicates().sort_values().head(max_examples).tolist()
    )


def _rule_rows(data_df: pd.DataFrame, flags: pd.DataFrame, rules: dict[str, schema.RuleDef], stage: str) -> list[dict[str, Any]]:
    """Convert rule flags into compact issue report rows."""
    rows: list[dict[str, Any]] = []
    denominator = len(data_df.index)
    for flag_column, rule in rules.items():
        if flag_column not in flags.columns:
            continue
        mask = flags[flag_column].fillna(False).astype(bool)
        count = int(mask.sum())
        if count == 0:
            continue
        rows.append(
            {
                "stage": stage,
                "issue_group": rule.issue_group,
                "rule_id": rule.rule_id,
                "field_path": rule.field_path,
                "description": rule.description,
                "count": count,
                "percent": round((count / denominator) * 100 if denominator else 0.0, 2),
                "severity": rule.severity,
                "example_application_ids": _example_ids(data_df["application_id"], mask),
            }
        )
    return rows


def _duplicate_rows(applications_df: pd.DataFrame, duplicate_report: pd.DataFrame, duplicate_metadata: pd.DataFrame, stage: str, ssn_column: str) -> list[dict[str, Any]]:
    """Build duplicate and repeated-SSN issue rows for the quality report."""
    rows: list[dict[str, Any]] = []
    denominator = len(applications_df.index)
    duplicate_rules = {item["rule_id"]: item for item in schema.DUPLICATE_RULES}
    dup_mask = duplicate_metadata["is_duplicate_id"].fillna(False).astype(bool)

    def append(rule_id: str, count: int, percent: float, examples: str) -> None:
        if count == 0:
            return
        item = duplicate_rules[rule_id]
        rows.append(
            {
                "stage": stage,
                "issue_group": item["issue_group"],
                "rule_id": rule_id,
                "field_path": item["field_path"],
                "description": item["description"],
                "count": int(count),
                "percent": round(percent, 2),
                "severity": item["severity"],
                "example_application_ids": examples,
            }
        )

    append("R_DUP_001", int(dup_mask.sum()), (float(dup_mask.sum()) / denominator) * 100 if denominator else 0.0, _example_ids(duplicate_metadata["application_id"], dup_mask))
    append("R_DUP_002", int(len(duplicate_report.index)), (float(len(duplicate_report.index)) / denominator) * 100 if denominator else 0.0, "|".join(duplicate_report["application_id"].astype(str).sort_values().head(5).tolist()))

    if ssn_column in applications_df.columns:
        ssn = applications_df[ssn_column].fillna("").astype(str).str.strip()
        non_blank = ssn.ne("")
        ssn_counts = ssn[non_blank].value_counts()
        duplicated_ssn_values = ssn_counts[ssn_counts > 1].index
        dup_ssn_mask = ssn.isin(duplicated_ssn_values)
        append("R_DUP_003", int(dup_ssn_mask.sum()), (float(dup_ssn_mask.sum()) / denominator) * 100 if denominator else 0.0, _example_ids(applications_df["application_id"], dup_ssn_mask))

        ssn_to_app = applications_df.loc[dup_ssn_mask, [ssn_column, "application_id"]].dropna(subset=[ssn_column]).assign(**{ssn_column: lambda frame: frame[ssn_column].astype(str).str.strip()})
        cross_app = ssn_to_app.groupby(ssn_column)["application_id"].nunique()
        cross_app_values = set(cross_app[cross_app > 1].index.tolist())
        cross_app_mask = ssn.isin(cross_app_values)
        append("R_DUP_004", int(len(cross_app_values)), (float(len(cross_app_values)) / denominator) * 100 if denominator else 0.0, _example_ids(applications_df["application_id"], cross_app_mask))

    return rows


def build_data_quality_report(*, applications_df: pd.DataFrame, application_flags: pd.DataFrame, duplicate_report: pd.DataFrame, duplicate_metadata: pd.DataFrame, spending_df: pd.DataFrame, spending_flags: pd.DataFrame, stage: str, rule_catalog: pd.DataFrame | None = None, ssn_column: str = "raw_applicant_ssn") -> pd.DataFrame:
    """Build a concise issue registry for one pipeline stage."""
    rows = []
    rows.extend(_rule_rows(applications_df, application_flags, schema.APPLICATION_RULES, stage))
    rows.extend(_duplicate_rows(applications_df, duplicate_report, duplicate_metadata, stage, ssn_column))
    rows.extend(_rule_rows(spending_df, spending_flags, schema.SPENDING_RULES, stage))

    report = pd.DataFrame(rows, columns=["stage", "issue_group", "rule_id", "field_path", "description", "count", "percent", "severity", "example_application_ids"])
    if report.empty:
        report["value_source"] = pd.Series(dtype="string")
        return report

    catalog = rule_catalog if rule_catalog is not None else schema.build_rule_catalog()
    report = report.merge(catalog[["stage", "rule_id", "value_source"]].drop_duplicates(), on=["stage", "rule_id"], how="left", validate="many_to_one")
    report["severity_order"] = report["severity"].map(SEVERITY_ORDER).fillna(99)
    report = report.sort_values(["stage", "severity_order", "count", "rule_id"], ascending=[True, True, False, True])
    return report.drop(columns="severity_order").reset_index(drop=True)


def _lookup_metric(report: pd.DataFrame, stage: str, rule_id: str) -> tuple[int, float, str]:
    """Return count, percent, and issue group for a rule from a staged quality report."""
    row = report.loc[(report["stage"] == stage) & (report["rule_id"] == rule_id)]
    if row.empty:
        return 0, 0.0, ""
    first = row.iloc[0]
    return int(first["count"]), float(first["percent"]), str(first["issue_group"])


def build_before_after_comparison(*, quality_report: pd.DataFrame, duplicate_report: pd.DataFrame, duplicate_metadata: pd.DataFrame, total_records: int, canonical_count: int) -> pd.DataFrame:
    """Build a compact before-vs-after remediation evidence table."""
    metrics = [
        ("R_APP_002", "Missing required applicant fields"),
        ("R_APP_005", "Invalid email format"),
        ("R_APP_006", "Gender requires standardisation"),
        ("R_APP_008", "DOB not in ISO format"),
        ("R_APP_009", "DOB ambiguity"),
        ("R_APP_010", "Annual income coercion issue"),
        ("R_APP_011", "Annual salary field drift"),
        ("R_APP_012", "Negative credit history months"),
        ("R_APP_013", "Negative savings balance"),
        ("R_APP_014", "Debt-to-income out of range"),
        ("R_APP_018", "Approved with credit history under 6 months"),
        ("R_SPN_001", "Spending missing category"),
        ("R_SPN_002", "Spending amount invalid"),
        ("R_SPN_003", "Spending amount negative"),
    ]

    rows: list[dict[str, Any]] = []
    for rule_id, metric_label in metrics:
        pre_count, pre_percent, issue_group = _lookup_metric(quality_report, "pre", rule_id)
        post_count, post_percent, issue_group_post = _lookup_metric(quality_report, "post", rule_id)
        rows.append(
            {
                "issue_group": issue_group or issue_group_post,
                "rule_id": rule_id,
                "metric_label": metric_label,
                "pre_count": pre_count,
                "post_count": post_count,
                "delta_count": post_count - pre_count,
                "pre_percent": round(pre_percent, 2),
                "post_percent": round(post_percent, 2),
                "delta_percent": round(post_percent - pre_percent, 2),
            }
        )

    duplicate_rows = int(duplicate_metadata["is_duplicate_id"].fillna(False).astype(bool).sum())
    conflict_ids = int(duplicate_report.loc[duplicate_report["classification"] == "conflict", "application_id"].nunique())
    rows.extend([
        {"issue_group": "Uniqueness", "rule_id": "R_DUP_001", "metric_label": "Duplicate application_id rows", "pre_count": duplicate_rows, "post_count": duplicate_rows, "delta_count": 0, "pre_percent": round((duplicate_rows / total_records) * 100 if total_records else 0.0, 2), "post_percent": round((duplicate_rows / total_records) * 100 if total_records else 0.0, 2), "delta_percent": 0.0},
        {"issue_group": "Uniqueness", "rule_id": "R_DUP_CONFLICT", "metric_label": "Duplicate conflict IDs", "pre_count": conflict_ids, "post_count": conflict_ids, "delta_count": 0, "pre_percent": round((conflict_ids / total_records) * 100 if total_records else 0.0, 2), "post_percent": round((conflict_ids / total_records) * 100 if total_records else 0.0, 2), "delta_percent": 0.0},
        {"issue_group": "Remediation", "rule_id": "R_DUP_CANONICAL", "metric_label": "Canonical rows retained for analysis", "pre_count": total_records, "post_count": canonical_count, "delta_count": canonical_count - total_records, "pre_percent": 100.0 if total_records else 0.0, "post_percent": round((canonical_count / total_records) * 100 if total_records else 0.0, 2), "delta_percent": round(((canonical_count / total_records) * 100 - 100.0) if total_records else 0.0, 2)},
    ])
    return pd.DataFrame(rows)
