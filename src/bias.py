"""
bias.py — Bias detection and fairness metrics for NovaCred credit application data.

Inputs (from Data Engineer outputs):
    - applications_analysis.csv
    - spending_items_clean.csv

All public functions return plain DataFrames or dicts so results can be
inspected, printed, or persisted directly from the notebook.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import scipy
from scipy import stats


# ── Constants ─────────────────────────────────────────────────────────────────

AGE_ORDER = ["<25", "25-34", "35-44", "45-54", "55-64", "65+"]
PALETTE = {"Female": "#E07B8B", "Male": "#4A90D9", "Unknown": "#AAAAAA"}
FOUR_FIFTHS_THRESHOLD = 0.80
PRIME_AGE_REFERENCE = "25-34"

FINANCIAL_COLS = [
    "clean_annual_income",
    "clean_credit_history_months",
    "clean_debt_to_income",
    "clean_savings_balance",
]


# ── Data loading ──────────────────────────────────────────────────────────────

def load_analysis(path: Path | str) -> pd.DataFrame:
    """Load and type-coerce applications_analysis.csv."""
    df = pd.read_csv(path, dtype={"clean_zip_code": str, "applicant_pseudo_id": str})

    df["clean_loan_approved"] = df["clean_loan_approved"].map(
        {True: True, False: False, "True": True, "False": False, 1: True, 0: False}
    )
    for col in [
        "clean_annual_income", "clean_credit_history_months",
        "clean_debt_to_income", "clean_savings_balance",
        "clean_interest_rate", "clean_approved_amount",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["approved"] = df["clean_loan_approved"].eq(True).astype("Int64")
    df["age_band"] = pd.Categorical(df["age_band"], categories=AGE_ORDER, ordered=True)
    return df


def load_spending(path: Path | str) -> pd.DataFrame:
    """Load spending_items_clean.csv."""
    df = pd.read_csv(path)
    if "amount_clean" in df.columns:
        df["amount_clean"] = pd.to_numeric(df["amount_clean"], errors="coerce")
    return df


# ── Core fairness metrics ─────────────────────────────────────────────────────

def approval_rate(df: pd.DataFrame) -> float:
    """Approval rate for a (pre-filtered) DataFrame, ignoring null outcomes."""
    valid = df["approved"].dropna()
    return float(valid.mean()) if len(valid) > 0 else float("nan")


def disparate_impact(
    df: pd.DataFrame,
    group_col: str,
    privileged: str,
    unprivileged: str,
) -> dict[str, Any]:
    """
    Compute the Disparate Impact Ratio and related fairness metrics.

    DI = approval_rate(unprivileged) / approval_rate(privileged)

    The four-fifths (80 %) rule flags DI < 0.80 as potential disparate impact.
    Returns a dict with rates, DI, DPD, and the four-fifths flag.
    """
    priv_df = df[df[group_col] == privileged]
    unpriv_df = df[df[group_col] == unprivileged]

    priv_rate = approval_rate(priv_df)
    unpriv_rate = approval_rate(unpriv_df)
    di = unpriv_rate / priv_rate if priv_rate > 0 else float("nan")
    dpd = unpriv_rate - priv_rate

    return {
        "privileged_group": privileged,
        "unprivileged_group": unprivileged,
        "privileged_n": len(priv_df),
        "unprivileged_n": len(unpriv_df),
        "privileged_rate": priv_rate,
        "unprivileged_rate": unpriv_rate,
        "disparate_impact": di,
        "demographic_parity_difference": dpd,
        "four_fifths_flag": bool(di < FOUR_FIFTHS_THRESHOLD) if not np.isnan(di) else False,
    }


def chi2_test(df: pd.DataFrame, group_col: str, outcome_col: str = "approved") -> dict[str, Any]:
    """Chi-squared test of independence between a group column and loan approval."""
    valid = df[[group_col, outcome_col]].dropna()
    ct = pd.crosstab(valid[group_col], valid[outcome_col])
    chi2, p, dof, _ = stats.chi2_contingency(ct)
    return {"chi2": round(chi2, 4), "p_value": round(p, 6), "dof": dof, "significant_at_05": bool(p < 0.05)}


def mannwhitney_test(a: pd.Series, b: pd.Series) -> dict[str, Any]:
    """Two-sided Mann-Whitney U test between two numeric series."""
    a, b = a.dropna(), b.dropna()
    if len(a) < 2 or len(b) < 2:
        return {"u_stat": float("nan"), "p_value": float("nan"), "significant_at_05": False}
    u, p = stats.mannwhitneyu(a, b, alternative="two-sided")
    return {"u_stat": round(u, 1), "p_value": round(p, 6), "significant_at_05": bool(p < 0.05)}


# ── Prepared subsets ──────────────────────────────────────────────────────────

def gender_subset(df: pd.DataFrame) -> pd.DataFrame:
    """Rows with a known binary gender and a non-null outcome."""
    return df[
        df["clean_gender"].isin(["Male", "Female"]) & df["approved"].notna()
    ].copy()


def age_subset(df: pd.DataFrame) -> pd.DataFrame:
    """Rows with a known age band and a non-null outcome."""
    return df[df["age_band"].notna() & df["approved"].notna()].copy()


# ── Approval rate tables ──────────────────────────────────────────────────────

def gender_approval_table(df: pd.DataFrame) -> pd.DataFrame:
    """Approval counts and rate by gender."""
    gdf = gender_subset(df)
    tbl = (
        gdf.groupby("clean_gender", observed=True)["approved"]
        .agg(n="count", approved_n="sum")
        .assign(approval_rate=lambda x: x["approved_n"] / x["n"])
        .reset_index()
        .rename(columns={"clean_gender": "gender"})
    )
    return tbl


def age_approval_table(df: pd.DataFrame) -> pd.DataFrame:
    """Approval counts and rate by age band (ordered)."""
    adf = age_subset(df)
    tbl = (
        adf.groupby("age_band", observed=True)["approved"]
        .agg(n="count", approved_n="sum")
        .assign(approval_rate=lambda x: x["approved_n"] / x["n"])
        .reset_index()
        .sort_values("age_band")
    )
    return tbl


def interaction_table(df: pd.DataFrame) -> pd.DataFrame:
    """Approval rate by age band × gender."""
    sub = df[
        df["clean_gender"].isin(["Male", "Female"]) &
        df["age_band"].notna() &
        df["approved"].notna()
    ].copy()
    tbl = (
        sub.groupby(["age_band", "clean_gender"], observed=True)["approved"]
        .agg(n="count", approved_n="sum")
        .assign(approval_rate=lambda x: x["approved_n"] / x["n"])
        .reset_index()
        .sort_values(["age_band", "clean_gender"])
    )
    return tbl


# ── Age DI table ──────────────────────────────────────────────────────────────

def age_di_table(df: pd.DataFrame, reference: str = PRIME_AGE_REFERENCE) -> pd.DataFrame:
    """DI ratios for every age band vs a reference band."""
    adf = age_subset(df)
    rows = []
    for band in AGE_ORDER:
        if band == reference or band not in adf["age_band"].values:
            continue
        r = disparate_impact(adf, "age_band", privileged=reference, unprivileged=band)
        rows.append(r)
    return pd.DataFrame(rows)


# ── Proxy discrimination ──────────────────────────────────────────────────────

def financial_proxy_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mann-Whitney U tests for each financial feature between Male and Female.
    Returns one row per feature with medians and significance.
    """
    gdf = gender_subset(df)
    rows = []
    for col in FINANCIAL_COLS:
        if col not in gdf.columns:
            continue
        males = gdf.loc[gdf["clean_gender"] == "Male", col]
        females = gdf.loc[gdf["clean_gender"] == "Female", col]
        result = mannwhitney_test(males, females)
        rows.append({
            "feature": col,
            "male_median": round(males.dropna().median(), 4),
            "female_median": round(females.dropna().median(), 4),
            "u_stat": result["u_stat"],
            "p_value": result["p_value"],
            "significant_at_05": result["significant_at_05"],
        })
    return pd.DataFrame(rows)


def spending_gender_table(analysis_df: pd.DataFrame, spending_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Average spending per category split by gender.
    Returns None if the necessary columns are absent.
    """
    cat_col = "category_clean" if "category_clean" in spending_df.columns else None
    amt_col = "amount_clean" if "amount_clean" in spending_df.columns else None
    if cat_col is None or amt_col is None:
        return None

    gender_ids = gender_subset(analysis_df)[["application_id", "clean_gender"]]
    merged = spending_df.merge(gender_ids, on="application_id", how="inner")
    if merged.empty:
        return None

    tbl = (
        merged.groupby(["clean_gender", cat_col], observed=True)[amt_col]
        .mean()
        .unstack(fill_value=0)
        .round(2)
    )
    return tbl


def credit_age_correlation(df: pd.DataFrame) -> dict[str, Any]:
    """Spearman correlation between age-band rank and credit history months."""
    adf = age_subset(df).copy()
    age_map = {b: i for i, b in enumerate(AGE_ORDER)}
    adf["age_band_rank"] = adf["age_band"].map(age_map)
    valid = adf[["age_band_rank", "clean_credit_history_months"]].dropna()
    rho, p = stats.spearmanr(valid["age_band_rank"], valid["clean_credit_history_months"])
    return {"spearman_rho": round(rho, 4), "p_value": round(p, 6), "significant_at_05": bool(p < 0.05)}


# ── Interest rate disparity ───────────────────────────────────────────────────

def interest_rate_by_gender(df: pd.DataFrame) -> dict[str, Any]:
    """
    Compare interest rates for approved Male vs Female applicants.
    Returns descriptive stats and a Mann-Whitney test.
    """
    approved = gender_subset(df)[df["approved"].eq(1)].copy() if "approved" in df.columns else pd.DataFrame()
    if approved.empty or "clean_interest_rate" not in approved.columns:
        return {}

    males = approved.loc[approved["clean_gender"] == "Male", "clean_interest_rate"]
    females = approved.loc[approved["clean_gender"] == "Female", "clean_interest_rate"]
    test = mannwhitney_test(males, females)

    return {
        "male_n": int(males.dropna().count()),
        "female_n": int(females.dropna().count()),
        "male_median_rate": round(males.dropna().median(), 6),
        "female_median_rate": round(females.dropna().median(), 6),
        "male_mean_rate": round(males.dropna().mean(), 6),
        "female_mean_rate": round(females.dropna().mean(), 6),
        **test,
    }


# ── Rejection reason breakdown ────────────────────────────────────────────────

def rejection_reason_by_gender(df: pd.DataFrame) -> pd.DataFrame | None:
    """Count of each rejection reason split by gender for rejected applicants."""
    gdf = gender_subset(df)
    rejected = gdf[gdf["approved"] == 0]
    if "clean_rejection_reason" not in rejected.columns or rejected.empty:
        return None
    tbl = (
        rejected.groupby(["clean_rejection_reason", "clean_gender"], observed=True)
        .size()
        .unstack(fill_value=0)
        .assign(total=lambda x: x.sum(axis=1))
        .sort_values("total", ascending=False)
    )
    return tbl


# ── Summary table ─────────────────────────────────────────────────────────────

def build_fairness_summary(
    gender_di: dict[str, Any],
    chi2_gender: dict[str, Any],
    age_di_df: pd.DataFrame,
    chi2_age: dict[str, Any],
    ir_result: dict[str, Any],
) -> pd.DataFrame:
    """Compile all fairness metrics into a single summary DataFrame."""
    rows = [
        {
            "analysis": "Gender — Disparate Impact Ratio",
            "metric_value": f"{gender_di['disparate_impact']:.4f}",
            "four_fifths_flag": gender_di["four_fifths_flag"],
            "p_value": chi2_gender["p_value"],
            "significant_at_05": chi2_gender["significant_at_05"],
            "note": f"Female rate {gender_di['unprivileged_rate']:.1%} vs Male {gender_di['privileged_rate']:.1%}",
        },
        {
            "analysis": "Gender — Demographic Parity Difference",
            "metric_value": f"{gender_di['demographic_parity_difference']:+.4f}",
            "four_fifths_flag": None,
            "p_value": chi2_gender["p_value"],
            "significant_at_05": chi2_gender["significant_at_05"],
            "note": "Negative = Female approval rate below Male",
        },
    ]

    for _, row in age_di_df.iterrows():
        rows.append({
            "analysis": f"Age — DI ratio ({row['unprivileged_group']} vs {PRIME_AGE_REFERENCE})",
            "metric_value": f"{row['disparate_impact']:.4f}",
            "four_fifths_flag": row["four_fifths_flag"],
            "p_value": chi2_age["p_value"],
            "significant_at_05": chi2_age["significant_at_05"],
            "note": f"n={int(row['unprivileged_n'])}",
        })

    if ir_result:
        rows.append({
            "analysis": "Interest Rate — Gender gap (approved only)",
            "metric_value": f"Male={ir_result['male_median_rate']:.4f} Female={ir_result['female_median_rate']:.4f}",
            "four_fifths_flag": None,
            "p_value": ir_result.get("p_value"),
            "significant_at_05": ir_result.get("significant_at_05"),
            "note": f"n Male={ir_result['male_n']}, n Female={ir_result['female_n']}",
        })

    return pd.DataFrame(rows)


# ── Plots ─────────────────────────────────────────────────────────────────────

def _save(fig: plt.Figure, path: Path | None) -> None:
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, bbox_inches="tight")


def plot_gender_di(gender_tbl: pd.DataFrame, di_result: dict[str, Any],
                   save_path: Path | None = None) -> plt.Figure:
    """Side-by-side: approval rate bars + DI gauge."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Left — approval rate bars
    ax = axes[0]
    genders = gender_tbl["gender"].tolist()
    rates = gender_tbl["approval_rate"].tolist()
    colors = [PALETTE.get(g, "#888") for g in genders]
    bars = ax.bar(genders, rates, color=colors, width=0.5, edgecolor="white")
    for bar, rate in zip(bars, rates):
        ax.text(bar.get_x() + bar.get_width() / 2, rate + 0.005,
                f"{rate:.1%}", ha="center", va="bottom", fontsize=11, fontweight="bold")
    ax.set_ylim(0, 1.0)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax.set_xlabel("Gender", fontsize=11)
    ax.set_ylabel("Loan Approval Rate", fontsize=11)
    ax.set_title("Approval Rate by Gender", fontsize=13, fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)

    # Right — DI gauge
    ax2 = axes[1]
    di_val = di_result["disparate_impact"]
    bar_color = "#E74C3C" if di_val < FOUR_FIFTHS_THRESHOLD else "#27AE60"
    ax2.barh(["DI Ratio"], [di_val], color=bar_color, height=0.35)
    ax2.axvline(FOUR_FIFTHS_THRESHOLD, color="black", linestyle="--", linewidth=1.5,
                label=f"4/5 threshold ({FOUR_FIFTHS_THRESHOLD:.2f})")
    ax2.axvline(1.0, color="gray", linestyle=":", linewidth=1.2, label="Perfect parity (1.00)")
    ax2.text(di_val + 0.01, 0, f"{di_val:.3f}", va="center", fontsize=13, fontweight="bold")
    ax2.set_xlim(0, 1.35)
    ax2.set_xlabel("Disparate Impact Ratio (Female / Male)", fontsize=11)
    ax2.set_title("Gender Disparate Impact Ratio", fontsize=13, fontweight="bold")
    ax2.legend(fontsize=10)
    ax2.spines[["top", "right"]].set_visible(False)

    flag_text = ("⚠  DI < 0.80 — Potential Disparate Impact" if di_val < FOUR_FIFTHS_THRESHOLD
                 else "✓  DI ≥ 0.80 — Within threshold")
    flag_color = "#E74C3C" if di_val < FOUR_FIFTHS_THRESHOLD else "#27AE60"
    fig.suptitle(flag_text, fontsize=12, color=flag_color, fontweight="bold", y=0.02)

    plt.tight_layout()
    _save(fig, save_path)
    return fig


def plot_age_approval(age_tbl: pd.DataFrame, save_path: Path | None = None) -> plt.Figure:
    """Approval rate by age band with reference and four-fifths threshold lines."""
    tbl = age_tbl.copy().sort_values("age_band")
    ref_rate = float(tbl.loc[tbl["age_band"] == PRIME_AGE_REFERENCE, "approval_rate"].iloc[0]) if PRIME_AGE_REFERENCE in tbl["age_band"].values else float("nan")

    fig, ax = plt.subplots(figsize=(11, 5))
    bands = tbl["age_band"].astype(str).tolist()
    rates = tbl["approval_rate"].tolist()
    ns = tbl["n"].tolist()

    bars = ax.bar(bands, rates, color="#5B9BD5", width=0.6, edgecolor="white")
    for bar, rate, n in zip(bars, rates, ns):
        ax.text(bar.get_x() + bar.get_width() / 2, rate + 0.008,
                f"{rate:.1%}", ha="center", va="bottom", fontsize=10, fontweight="bold")
        ax.text(bar.get_x() + bar.get_width() / 2, 0.02,
                f"n={n}", ha="center", va="bottom", fontsize=8, color="white", fontweight="bold")

    if not np.isnan(ref_rate):
        ax.axhline(ref_rate, color="#2ECC71", linestyle="--", linewidth=1.5,
                   label=f"{PRIME_AGE_REFERENCE} rate (reference)")
        ax.axhline(ref_rate * FOUR_FIFTHS_THRESHOLD, color="#E74C3C", linestyle=":",
                   linewidth=1.5, label="80 % of reference (DI threshold)")

    ax.set_ylim(0, 1.0)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax.set_xlabel("Age Band", fontsize=11)
    ax.set_ylabel("Loan Approval Rate", fontsize=11)
    ax.set_title("Approval Rate by Age Band", fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)

    plt.tight_layout()
    _save(fig, save_path)
    return fig


def plot_interaction_heatmap(interaction_tbl: pd.DataFrame, save_path: Path | None = None) -> plt.Figure:
    """Heatmap of approval rate by gender × age band."""
    heat = interaction_tbl.pivot(index="age_band", columns="clean_gender", values="approval_rate")
    heat = heat.reindex(AGE_ORDER)
    n_pivot = interaction_tbl.pivot(index="age_band", columns="clean_gender", values="n").reindex(AGE_ORDER)

    fig, ax = plt.subplots(figsize=(7, 5))
    im = ax.imshow(heat.values.astype(float), cmap="RdYlGn", aspect="auto", vmin=0, vmax=1)

    ax.set_xticks(range(len(heat.columns)))
    ax.set_xticklabels(heat.columns.tolist(), fontsize=12)
    ax.set_yticks(range(len(heat.index)))
    ax.set_yticklabels(heat.index.astype(str).tolist(), fontsize=11)

    for i in range(heat.shape[0]):
        for j in range(heat.shape[1]):
            val = heat.values[i, j]
            if not np.isnan(float(val)):
                n_val = n_pivot.values[i, j]
                n_str = f"\nn={int(n_val)}" if not np.isnan(float(n_val)) else ""
                text_color = "black" if 0.3 < float(val) < 0.75 else "white"
                ax.text(j, i, f"{float(val):.1%}{n_str}",
                        ha="center", va="center", fontsize=10, color=text_color)

    plt.colorbar(im, ax=ax, label="Approval Rate", format="{x:.0%}")
    ax.set_title("Approval Rate: Gender × Age Band", fontsize=13, fontweight="bold")
    ax.set_xlabel("Gender", fontsize=11)
    ax.set_ylabel("Age Band", fontsize=11)

    plt.tight_layout()
    _save(fig, save_path)
    return fig


def plot_interaction_bars(interaction_tbl: pd.DataFrame, save_path: Path | None = None) -> plt.Figure:
    """Grouped bars: approval rate by age band, split by gender."""
    fig, ax = plt.subplots(figsize=(12, 5))
    bands = [b for b in AGE_ORDER if b in interaction_tbl["age_band"].astype(str).values]
    x = np.arange(len(bands))
    width = 0.35

    for i, g in enumerate(["Female", "Male"]):
        g_data = (interaction_tbl[interaction_tbl["clean_gender"] == g]
                  .set_index("age_band")["approval_rate"])
        vals = [float(g_data.get(b, float("nan"))) for b in bands]
        rects = ax.bar(x + (i - 0.5) * width, vals, width,
                       label=g, color=PALETTE[g], edgecolor="white", alpha=0.9)
        for rect, v in zip(rects, vals):
            if not np.isnan(v):
                ax.text(rect.get_x() + rect.get_width() / 2, v + 0.01,
                        f"{v:.0%}", ha="center", va="bottom", fontsize=8.5)

    ax.set_xticks(x)
    ax.set_xticklabels(bands, fontsize=11)
    ax.set_ylim(0, 1.15)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax.set_xlabel("Age Band", fontsize=11)
    ax.set_ylabel("Approval Rate", fontsize=11)
    ax.set_title("Approval Rate by Age Band and Gender", fontsize=13, fontweight="bold")
    ax.legend(title="Gender", fontsize=10)
    ax.spines[["top", "right"]].set_visible(False)

    plt.tight_layout()
    _save(fig, save_path)
    return fig


def plot_financial_boxplots(df: pd.DataFrame, save_path: Path | None = None) -> plt.Figure:
    """Box plots of key financial features by gender (proxy discrimination check)."""
    gdf = gender_subset(df)
    plot_feats = [
        ("clean_annual_income",         "Annual Income ($)"),
        ("clean_credit_history_months", "Credit History (months)"),
        ("clean_debt_to_income",        "Debt-to-Income Ratio"),
    ]
    plot_feats = [(c, l) for c, l in plot_feats if c in gdf.columns]

    fig, axes = plt.subplots(1, len(plot_feats), figsize=(5 * len(plot_feats), 5))
    if len(plot_feats) == 1:
        axes = [axes]

    for ax, (col, label) in zip(axes, plot_feats):
        groups = [gdf.loc[gdf["clean_gender"] == g, col].dropna().values
                  for g in ["Female", "Male"]]
        bp = ax.boxplot(groups, labels=["Female", "Male"], patch_artist=True,
                        medianprops={"color": "black", "linewidth": 2})
        for patch, color in zip(bp["boxes"], [PALETTE["Female"], PALETTE["Male"]]):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        ax.set_title(label, fontsize=11, fontweight="bold")
        ax.set_xlabel("Gender")
        ax.spines[["top", "right"]].set_visible(False)

    fig.suptitle("Financial Feature Distribution by Gender (Proxy Check)",
                 fontsize=12, fontweight="bold")
    plt.tight_layout()
    _save(fig, save_path)
    return fig


def plot_interest_rate(df: pd.DataFrame, save_path: Path | None = None) -> plt.Figure:
    """Overlapping histograms of interest rates for approved applicants by gender."""
    approved = gender_subset(df)[gender_subset(df)["approved"].eq(1)]
    if "clean_interest_rate" not in approved.columns:
        fig, ax = plt.subplots(); ax.set_title("No interest rate data"); return fig

    fig, ax = plt.subplots(figsize=(9, 5))
    for g, color in [("Female", PALETTE["Female"]), ("Male", PALETTE["Male"])]:
        vals = approved.loc[approved["clean_gender"] == g, "clean_interest_rate"].dropna()
        if len(vals) > 0:
            ax.hist(vals, bins=15, alpha=0.6, color=color,
                    label=f"{g} (n={len(vals)})", edgecolor="white")
            ax.axvline(vals.median(), color=color, linestyle="--", linewidth=2)

    ax.set_xlabel("Interest Rate", fontsize=11)
    ax.set_ylabel("Count", fontsize=11)
    ax.set_title("Interest Rate Distribution by Gender\n(approved applicants; dashed = median)",
                 fontsize=12, fontweight="bold")
    ax.legend(fontsize=10)
    ax.spines[["top", "right"]].set_visible(False)

    plt.tight_layout()
    _save(fig, save_path)
    return fig
