import os
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def to_month_dt(s: str):
    try:
        return pd.to_datetime(s + "-01", format="%Y-%m-%d")
    except Exception:
        try:
            return pd.to_datetime(s)
        except Exception:
            return pd.NaT

def map_origin_category(origin: str) -> str:
    if not isinstance(origin, str):
        return "Other"
    o = origin.lower()
    # Heuristiken
    if any(k in o for k in ["private", "rsa", "ecdsa", "ed25519", "ssh", "private_key", "pgp", "gpg", "pem", "pkey"]):
        return "Private Keys"
    if any(k in o for k in ["token", "oauth", "bearer", "github", "gitlab", "gh_token"]):
        return "Tokens"
    if any(k in o for k in ["aws", "s3", "access_key", "secret_key", "gcp", "google", "azure", "cloud", "firebase", "twilio", "stripe", "slack", "sendgrid"]):
        return "Cloud/API Keys"
    if any(k in o for k in ["password", "passwd", "cred", "credential", "db_", "mysql", "postgres", "mongo"]):
        return "Credentials"
    return "Other"

def style():
    # kein Grid
    sns.set_theme(style="white", context="talk")
    plt.rcParams.update({
        "figure.figsize": (12, 6),
        "axes.titleweight": "bold",
        "axes.labelsize": 14,
        "axes.titlesize": 16,
        "legend.frameon": False,
        "savefig.dpi": 200,
        "axes.grid": False
    })

def plot_rq1(repos_csv: str, outdir: str):
    df = pd.read_csv(repos_csv)
    if df.empty:
        return
    df["month"] = df["month_key"].apply(to_month_dt)
    df = df.dropna(subset=["month"]).sort_values("month")
    # Zeitraum 2016–2025
    df = df[(df["month"] >= pd.Timestamp("2016-01-01")) & (df["month"] < pd.Timestamp("2026-01-01"))]

    # kumulierte gewichtete Summen
    df["cum_total_w"] = df["total_repos_weighted"].fillna(0).cumsum()
    df["cum_secret_w"] = df["repos_with_secret_weighted"].fillna(0).cumsum()

    # Anteil (monatlich, gewichtet) für Hintergrund
    with np.errstate(divide="ignore", invalid="ignore"):
        share = df["repos_with_secret_weighted"] / df["total_repos_weighted"]
    share = share.replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)
    df["share_w"] = share

    fig, ax_left = plt.subplots()
    ax_left.grid(False)

    # Hintergrundfläche: Anteil (auf linke Skala skaliert)
    scale = float(df["cum_total_w"].max()) if df["cum_total_w"].max() > 0 else 1.0
    ax_left.fill_between(df["month"], 0, df["share_w"] * scale, color="lightgray", alpha=0.25, linewidth=0, zorder=0)

    # Linke Achse: kumulativ total (gewichtet)
    line_total, = ax_left.plot(df["month"], df["cum_total_w"], color="#1f77b4", label="Total (kumuliert, gewichtet)")

    # Rechte Achse: kumulativ mit Secret (gewichtet)
    ax_right = ax_left.twinx()
    ax_right.grid(False)
    line_secret, = ax_right.plot(df["month"], df["cum_secret_w"], color="#d62728", label="Mit Secret (kumuliert, gewichtet)")

    # Ereignislinien (optional)
    ax_left.axvline(pd.Timestamp("2016-01-01"), color="gray", linestyle="--", linewidth=1)
    ax_left.axvline(pd.Timestamp("2023-05-01"), color="gray", linestyle="--", linewidth=1)

    ax_left.set_xlabel("Monat")
    ax_left.set_ylabel("Kumulierte gewichtete Repos (Total)")
    ax_right.set_ylabel("Kumulierte gewichtete Repos (mit Secret)")

    # kombinierte Legende
    handles = [line_total, line_secret]
    labels = [h.get_label() for h in handles]
    ax_left.legend(handles, labels, loc="upper left")

    ensure_dir(outdir)
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "RQ1_repo_share_with_secrets_timeseries.eps"), bbox_inches="tight")
    fig.savefig(os.path.join(outdir, "RQ1_repo_share_with_secrets_timeseries.png"), bbox_inches="tight")
    plt.close(fig)

def plot_rq2(top20_csv: str, outdir: str):
    # Wenn komplette Häufigkeiten vorhanden, nutze diese
    all_csv = os.path.join(os.path.dirname(top20_csv), "secret_origin_frequency.csv")
    src_csv = all_csv if os.path.exists(all_csv) else top20_csv
    df = pd.read_csv(src_csv)
    if df.empty:
        return

    # Spaltennamen normalisieren
    if "n" not in df.columns and "count" in df.columns:
        df = df.rename(columns={"count": "n"})
    if "origin" not in df.columns:
        for c in df.columns:
            if c.lower() in ("origin", "secret_origin"):
                df = df.rename(columns={c: "origin"})
                break
    df = df[["origin", "n"]].copy()
    df = df.sort_values("n", ascending=False)

    # Barplot (horizontale Balken), exakte Werte an Balken
    fig, ax = plt.subplots(figsize=(12, max(6, 0.35 * len(df))))
    ax.grid(False)
    sns.barplot(data=df, y="origin", x="n", ax=ax, color="#1f77b4")
    ax.set_xlabel("Anzahl Vorkommen")
    ax.set_ylabel("Origin")
    for p, val in zip(ax.patches, df["n"].tolist()):
        width = p.get_width()
        y = p.get_y() + p.get_height() / 2
        ax.text(width, y, f" {int(val)}", va="center", ha="left", fontsize=11, color="black")

    fig.tight_layout()
    ensure_dir(outdir)
    fig.savefig(os.path.join(outdir, "RQ2_secret_origins_bar.eps"), bbox_inches="tight")
    fig.savefig(os.path.join(outdir, "RQ2_secret_origins_bar.png"), bbox_inches="tight")
    plt.close(fig)

    # Kategorien (optional)
    df["category"] = df["origin"].apply(map_origin_category)
    df_cat = df.groupby("category", as_index=False)["n"].sum().sort_values("n", ascending=False)
    fig, ax = plt.subplots()
    ax.grid(False)
    sns.barplot(data=df_cat, x="category", y="n", ax=ax, palette="tab10")
    ax.set_xlabel("Kategorie")
    ax.set_ylabel("Anzahl Vorkommen")
    for tick in ax.get_xticklabels():
        tick.set_rotation(20)
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "RQ2_categories_bar.eps"), bbox_inches="tight")
    fig.savefig(os.path.join(outdir, "RQ2_categories_bar.png"), bbox_inches="tight")
    plt.close(fig)

def plot_rq3(dist_csv: str, outdir: str):
    df = pd.read_csv(dist_csv)
    if df.empty:
        return
    df = df.sort_values("repo_count")

    # Barplot (log y)
    fig, ax = plt.subplots()
    ax.grid(False)
    sns.barplot(data=df, x="repo_count", y="secret_count", ax=ax, color="#1f77b4")
    ax.set_yscale("log")
    ax.set_xlabel("Repos pro Secret")
    ax.set_ylabel("Anzahl Secrets (log)")
    fig.tight_layout()
    ensure_dir(outdir)
    fig.savefig(os.path.join(outdir, "RQ3_repo_count_per_secret_hist.eps"), bbox_inches="tight")
    fig.savefig(os.path.join(outdir, "RQ3_repo_count_per_secret_hist.png"), bbox_inches="tight")
    plt.close(fig)

    # CCDF
    df = df.sort_values("repo_count")
    df["cum_ge"] = df["secret_count"][::-1].cumsum()[::-1]
    fig, ax = plt.subplots()
    ax.grid(False)
    ax.plot(df["repo_count"], df["cum_ge"] / df["secret_count"].sum(), marker="o")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Repos pro Secret (x)")
    ax.set_ylabel("P(Repos≥x)")
    fig.tight_layout()
    ensure_dir(outdir)
    fig.savefig(os.path.join(outdir, "RQ3_repo_count_per_secret_ccdf.eps"), bbox_inches="tight")
    fig.savefig(os.path.join(outdir, "RQ3_repo_count_per_secret_ccdf.png"), bbox_inches="tight")
    plt.close(fig)

def plot_rq4(metrics_csv: str, contingency_dir: str, outdir: str):
    dfm = pd.read_csv(metrics_csv)
    if dfm.empty:
        return
    dfm["abs_value"] = dfm["value"].abs()
    dfm = dfm.sort_values("abs_value", ascending=False)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.grid(False)
    sns.barplot(data=dfm, x="variable", y="abs_value", hue="type", ax=ax)
    ax.set_xlabel("Variable")
    ax.set_ylabel("Stärke (|Korrelation| bzw. Cramér’s V)")
    for tick in ax.get_xticklabels():
        tick.set_rotation(20)
    ax.legend(title="")
    fig.tight_layout()
    ensure_dir(outdir)
    fig.savefig(os.path.join(outdir, "RQ4_association_strengths.eps"), bbox_inches="tight")
    fig.savefig(os.path.join(outdir, "RQ4_association_strengths.png"), bbox_inches="tight")
    plt.close(fig)

    # Heatmaps für kategoriale Kontingenz-Tabellen (falls vorhanden)
    for col in ["status_cat", "is_official_cat", "is_automated_cat"]:
        path = os.path.join(contingency_dir, f"contingency_{col}.csv")
        if not os.path.exists(path):
            continue
        dfc = pd.read_csv(path)
        if dfc.empty:
            continue
        dfc = dfc.set_index(col)
        fig, ax = plt.subplots(figsize=(8, 4 + 0.4 * len(dfc)))
        ax.grid(False)
        sns.heatmap(dfc, annot=True, fmt=".0f", cmap="Blues", ax=ax)
        fig.tight_layout()
        fig.savefig(os.path.join(outdir, f"RQ4_contingency_{col}.eps"), bbox_inches="tight")
        fig.savefig(os.path.join(outdir, f"RQ4_contingency_{col}.png"), bbox_inches="tight")
        plt.close(fig)

def main():
    parser = argparse.ArgumentParser(description="Plots für RQ1–RQ4 aus vorberechneten CSVs.")
    parser.add_argument("--empiric-dir", default="scripts/python_scripts/Analyse/empiric_outputs", help="Verzeichnis mit empirischen CSVs")
    parser.add_argument("--outdir", default="scripts/python_scripts/Analyse/figures", help="Zielverzeichnis für Abbildungen")
    args = parser.parse_args()

    style()
    ensure_dir(args.outdir)

    # RQ1
    plot_rq1(
        repos_csv=os.path.join(args.empiric_dir, "repos_with_secret_per_month.csv"),
        outdir=args.outdir
    )
    # RQ2
    plot_rq2(
        top20_csv=os.path.join(args.empiric_dir, "top20_secret_origin.csv"),
        outdir=args.outdir
    )
    # RQ3
    plot_rq3(
        dist_csv=os.path.join(args.empiric_dir, "secret_repo_counts_distribution.csv"),
        outdir=args.outdir
    )
    # RQ4
    plot_rq4(
        metrics_csv=os.path.join(args.empiric_dir, "metadata_association_metrics.csv"),
        contingency_dir=args.empiric_dir,
        outdir=args.outdir
    )

if __name__ == "__main__":
    main()