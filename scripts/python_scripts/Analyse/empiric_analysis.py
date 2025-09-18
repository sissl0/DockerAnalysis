import os
import sys
import argparse
import math
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

def load_month_bias(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    df = pd.read_csv(path)
    if "key" in df.columns and "bias_full_over_sample" in df.columns:
        return {str(k): float(v) for k, v in zip(df["key"], df["bias_full_over_sample"])}
    if "p_full" in df.columns and "p_sample" in df.columns and "key" in df.columns:
        w = []
        for pf, ps in zip(df["p_full"], df["p_sample"]):
            w.append((pf / ps) if ps and ps > 0 else (math.inf if pf and pf > 0 else 1.0))
        df["bias_full_over_sample"] = w
        return {str(k): float(v) for k, v in zip(df["key"], df["bias_full_over_sample"])}
    return {}

def load_bias_map_from_csv(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    df = pd.read_csv(path)
    if {"key", "bias_full_over_sample"}.issubset(df.columns):
        return {str(k): float(v) for k, v in zip(df["key"], df["bias_full_over_sample"])}
    return {}

def load_bias_bins_csv(path: str) -> pd.DataFrame | None:
    if not path or not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    cols = {"bin_left", "bin_right", "bias_full_over_sample"}
    if not cols.issubset(df.columns):
        return None
    df = df.sort_values(["bin_left", "bin_right"]).reset_index(drop=True)
    return df

def map_value_to_bin_weight(value: float, bins_df: pd.DataFrame | None) -> float:
    if bins_df is None or not np.isfinite(value):
        return 1.0
    # links-inklusive, rechts-exklusive, letztes Intervall rechts-inklusive
    for i, row in bins_df.iterrows():
        left = row["bin_left"]
        right = row["bin_right"]
        if (value >= left) and (value < right or (i == len(bins_df) - 1 and value <= right)):
            w = row["bias_full_over_sample"]
            try:
                return float(w) if np.isfinite(w) and w > 0 else 1.0
            except Exception:
                return 1.0
    return 1.0

def month_str(ts) -> str:
    ts = pd.to_datetime(ts, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return f"{ts.year:04d}-{ts.month:02d}"

def normalize_status(s) -> str:
    if s is None:
        return "unknown"
    s = str(s).strip()
    return s if s else "unknown"

def weighted_corr(x: np.ndarray, y: np.ndarray, w: np.ndarray) -> float:
    m = np.sum(w)
    if m <= 0:
        return float("nan")
    mx = np.sum(w * x) / m
    my = np.sum(w * y) / m
    vx = np.sum(w * (x - mx) * (x - mx)) / m
    vy = np.sum(w * (y - my) * (y - my)) / m
    if vx <= 0 or vy <= 0:
        return float("nan")
    cov = np.sum(w * (x - mx) * (y - my)) / m
    return float(cov / math.sqrt(vx * vy))

def weighted_cramers_v(ct: pd.DataFrame) -> float:
    n = ct.values.sum()
    if n <= 0:
        return float("nan")
    row_sums = ct.sum(axis=1).values.reshape(-1, 1)
    col_sums = ct.sum(axis=0).values.reshape(1, -1)
    expected = row_sums @ col_sums / n
    with np.errstate(divide="ignore", invalid="ignore"):
        chi2 = np.nansum((ct.values - expected) ** 2 / np.where(expected == 0, np.nan, expected))
    r, c = ct.shape
    denom = n * (min(r - 1, c - 1))
    if denom <= 0:
        return float("nan")
    return float(math.sqrt(chi2 / denom))

def load_unique_layer_bias_from_csv(path: str, fallback_full=0.2820) -> tuple[float, float, float, float]:
    # Returns w_unique, w_nonunique, f_unique, s_unique
    if path and os.path.exists(path):
        try:
            df = pd.read_csv(path)
            f_unique = float(df.get("unique_share_full_reference", [fallback_full])[0])
            s_unique = float(df.get("unique_share_sample", [np.nan])[0])
            if not np.isfinite(s_unique) or s_unique <= 0 or s_unique >= 1:
                return 1.0, 1.0, f_unique, float("nan")
            s_non = 1.0 - s_unique
            f_non = 1.0 - f_unique
            w_unique = f_unique / s_unique if s_unique > 0 else 1.0
            w_nonunique = f_non / s_non if s_non > 0 else 1.0
            return w_unique, w_nonunique, f_unique, s_unique
        except Exception:
            pass
    return 1.0, 1.0, fallback_full, float("nan")

def set_pg_session_params(conn, seed: float):
    # Weniger Temp-Datei-Druck, mehr Arbeit im RAM, deterministische random()-Auswahl
    conn.execute(text("SET max_parallel_workers_per_gather = 0"))
    conn.execute(text("SET work_mem = '256MB'"))
    conn.execute(text("SET temp_buffers = '64MB'"))
    conn.execute(text("SET enable_hashagg = on"))
    conn.execute(text("SELECT setseed(:s)"), {"s": seed})

def main():
    ap = argparse.ArgumentParser(description="Empirical analyses with bias corrections; outputs CSVs.")
    ap.add_argument("--outdir", default="empiric_outputs", help="Output directory for CSVs")
    ap.add_argument("--db-url", default=os.environ.get("DATABASE_URL", ""), help="Postgres URL")
    ap.add_argument("--bias-dir", default="scripts/python_scripts/Analyse/outputs", help="Directory with bias CSVs from validation")
    ap.add_argument("--setseed", type=float, default=0.42, help="Random seed for per-repo tag selection")
    args = ap.parse_args()

    if not args.db_url:
        print("DATABASE_URL not set and --db-url not given.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.outdir, exist_ok=True)
    engine = create_engine(args.db_url)

    # Session initialisieren
    with engine.begin() as conn:
        set_pg_session_params(conn, args.setseed)

    # Load biases from outputs dir
    month_bias = load_month_bias(os.path.join(args.bias_dir, "bias_last_pushed_month.csv"))
    status_bias = load_bias_map_from_csv(os.path.join(args.bias_dir, "bias_status.csv"))
    isoff_bias = load_bias_map_from_csv(os.path.join(args.bias_dir, "bias_is_official.csv"))
    pull_bins = load_bias_bins_csv(os.path.join(args.bias_dir, "bias_pull_count_bins.csv"))
    w_unique, w_nonunique, f_unique, s_unique = load_unique_layer_bias_from_csv(os.path.join(args.bias_dir, "unique_layer_share.csv"))

    # Per-Repo zufälliges Tag + Repo-Metadaten + has_secret
    sql_repo = text("""
        WITH per_repo AS (
          SELECT
            r.id AS repo_id,
            r.pull_count,
            r.star_count,
            r.is_official,
            r.is_automated,
            t1.last_pushed AS last_pushed,
            t1.status AS status,
            date_trunc('month', t1.last_pushed)::date AS month_assigned
          FROM repositories r
          LEFT JOIN LATERAL (
            SELECT t.last_pushed, t.status
            FROM tags t
            WHERE t.repo_id = r.id AND t.last_pushed IS NOT NULL
            ORDER BY random()
            LIMIT 1
          ) t1 ON TRUE
        ),
        has_secret AS (
          SELECT DISTINCT rl.repo_id
          FROM repo_layers rl
          JOIN layer_secret_occurrences lso ON lso.layer_id = rl.layer_id
        )
        SELECT p.*,
               (hs.repo_id IS NOT NULL) AS has_secret
        FROM per_repo p
        LEFT JOIN has_secret hs ON hs.repo_id = p.repo_id
        WHERE p.month_assigned IS NOT NULL
    """)
    with engine.connect() as conn:
        df_repo = pd.read_sql_query(sql_repo, conn)

    # Basismapping und Gewichte
    df_repo["month_key"] = pd.to_datetime(df_repo["month_assigned"]).map(month_str)
    df_repo["status_cat"] = df_repo["status"].map(normalize_status)
    df_repo["is_official_cat"] = np.where(df_repo["is_official"].astype(bool), "official", "unofficial")

    df_repo["w_month"] = df_repo["month_key"].map(month_bias).fillna(1.0).astype(float)
    df_repo["w_status"] = df_repo["status_cat"].map(status_bias).fillna(1.0).astype(float)
    df_repo["w_isoff"] = df_repo["is_official_cat"].map(isoff_bias).fillna(1.0).astype(float)
    df_repo["w_pull"] = df_repo["pull_count"].apply(lambda v: map_value_to_bin_weight(float(v) if pd.notna(v) else np.nan, pull_bins)).astype(float)

    # Gesamtes Repo-Gewicht (vorsichtig multiplizieren, harte Kappung)
    w = df_repo["w_month"] * df_repo["w_status"] * df_repo["w_isoff"] * df_repo["w_pull"]
    df_repo["weight"] = np.clip(w.replace([np.inf, -np.inf], 1.0).fillna(1.0), 0, 100.0)

    df_repo["has_secret"] = df_repo["has_secret"].astype(bool)

    # 1) Repos mit >=1 Secret pro Monat + gesamte Repos (ungewichtet + gewichtet)
    total = df_repo.groupby("month_key")["repo_id"].nunique().rename("total_repos")
    w_total = df_repo.groupby("month_key")["weight"].sum().rename("total_repos_weighted")

    sec = df_repo[df_repo["has_secret"]].groupby("month_key")["repo_id"].nunique().rename("repos_with_secret")
    w_sec = df_repo[df_repo["has_secret"]].groupby("month_key")["weight"].sum().rename("repos_with_secret_weighted")

    df1 = (
        total.reset_index()
        .merge(w_total, on="month_key", how="left")
        .merge(sec, on="month_key", how="left")
        .merge(w_sec, on="month_key", how="left")
        .fillna({
            "total_repos_weighted": 0,
            "repos_with_secret": 0,
            "repos_with_secret_weighted": 0
        })
        .sort_values("month_key")
    )
    df1.to_csv(os.path.join(args.outdir, "repos_with_secret_per_month.csv"), index=False)
    
    # 2) Top 20 Arten von Secrets (origin)
    with engine.connect() as conn:
        df2 = pd.read_sql_query(text("""
            SELECT COALESCE(NULLIF(TRIM(s.origin), ''), 'unknown') AS origin,
                   COUNT(*) AS n
            FROM layer_secret_occurrences lso
            JOIN secrets_filtered s ON s.id = lso.secret_id
            GROUP BY 1
            ORDER BY n DESC
            LIMIT 20
        """), conn)
    df2.to_csv(os.path.join(args.outdir, "top20_secret_origin.csv"), index=False)

    # 3) Identisches Secret in verschiedenen Repos (duplikatarme Variante)
    # Vorab DISTINCT auf (layer_id, secret_id) und (layer_id, repo_id), dann ohne COUNT(DISTINCT) zählen
    with engine.connect() as conn:
        df3 = pd.read_sql_query(text("""
            WITH rl_dist AS (
              SELECT DISTINCT layer_id, repo_id
              FROM repo_layers
            ),
            lso_dist AS (
              SELECT DISTINCT layer_id, secret_id
              FROM layer_secret_occurrences
            ),
            secret_repo AS (
              SELECT lso_dist.secret_id, rl_dist.repo_id
              FROM lso_dist
              JOIN rl_dist USING (layer_id)
              GROUP BY lso_dist.secret_id, rl_dist.repo_id
            )
            SELECT s.fragment_hash, COUNT(*) AS repo_count
            FROM secret_repo sr
            JOIN secrets_filtered s ON s.id = sr.secret_id
            GROUP BY s.fragment_hash
        """), conn)

    df3_dist = df3.groupby("repo_count")["fragment_hash"].nunique().reset_index(name="secret_count")
    df3.to_csv(os.path.join(args.outdir, "secret_repo_counts_per_secret.csv"), index=False)
    df3_dist.to_csv(os.path.join(args.outdir, "secret_repo_counts_distribution.csv"), index=False)

    # 4) Secret über Repos und Layer; Layer nach Unique-Layer-Bias gewichten (aus CSV)
    with engine.connect() as conn:
        df_lu = pd.read_sql_query(text("""
            WITH layer_repo_counts AS (
                SELECT rl.layer_id, COUNT(DISTINCT rl.repo_id) AS repo_cnt
                FROM repo_layers rl
                GROUP BY rl.layer_id
            )
            SELECT layer_id, (CASE WHEN repo_cnt = 1 THEN 1 ELSE 0 END) AS is_unique
            FROM layer_repo_counts
        """), conn)
        df4a = pd.read_sql_query(text("""
            SELECT s.fragment_hash,
                   COUNT(DISTINCT rl.repo_id) AS repo_count,
                   COUNT(DISTINCT lso.layer_id) AS layer_count
            FROM secrets_filtered s
            JOIN layer_secret_occurrences lso ON lso.secret_id = s.id
            JOIN repo_layers rl ON rl.layer_id = lso.layer_id
            GROUP BY s.fragment_hash
        """), conn)
        df4b = pd.read_sql_query(text("""
            SELECT s.fragment_hash, lso.layer_id
            FROM secrets_filtered s
            JOIN layer_secret_occurrences lso ON lso.secret_id = s.id
            GROUP BY s.fragment_hash, lso.layer_id
        """), conn)

    df4 = df4a.merge(df4b.merge(df_lu, on="layer_id", how="left"), on="fragment_hash", how="left")
    df4["layer_weight"] = np.where(df4["is_unique"] == 1, w_unique, w_nonunique).astype(float)
    df4_weighted = df4.groupby("fragment_hash")["layer_weight"].sum().reset_index(name="weighted_layer_count")
    df4_full = df4a.merge(df4_weighted, on="fragment_hash", how="left")
    df4_full.to_csv(os.path.join(args.outdir, "secret_repo_layer_counts_weighted.csv"), index=False)

    # 5) Zusammenhänge zwischen Secrets und Metadaten (gewichtet mit Bias)
    df_num = df_repo.copy()
    df_num["y"] = df_num["has_secret"].astype(float)
    df_num["last_pushed_epoch"] = pd.to_datetime(df_num["last_pushed"]).astype("int64") / 1e9
    metrics = []
    for col in ["pull_count", "star_count", "last_pushed_epoch"]:
        x = pd.to_numeric(df_num[col], errors="coerce").to_numpy(dtype=float)
        y = df_num["y"].to_numpy(dtype=float)
        w = df_repo["weight"].to_numpy(dtype=float)
        mask = np.isfinite(x) & np.isfinite(y) & np.isfinite(w) & (w > 0)
        r = weighted_corr(x[mask], y[mask], w[mask]) if mask.sum() > 1 else float("nan")
        metrics.append({"variable": col, "type": "numeric_corr_weighted", "value": r})
    # Kategorisch: status, is_official, is_automated -> weighted Cramer's V
    df_cat = df_repo.copy()
    df_cat["is_automated_cat"] = np.where(df_cat["is_automated"].astype(bool), "automated", "manual")
    for col in ["status_cat", "is_official_cat", "is_automated_cat"]:
        ct = df_cat.pivot_table(index=col, columns="has_secret", values="weight", aggfunc="sum", fill_value=0.0)
        v = weighted_cramers_v(ct)
        metrics.append({"variable": col, "type": "categorical_cramers_v_weighted", "value": v})
        ct_out = ct.copy()
        ct_out.columns = [f"has_secret={c}" for c in ct_out.columns]
        ct_out.reset_index().to_csv(os.path.join(args.outdir, f"contingency_{col}.csv"), index=False)
    pd.DataFrame(metrics).to_csv(os.path.join(args.outdir, "metadata_association_metrics.csv"), index=False)

    # 6) Filetype-Häufigkeit, FileSize, Layer-Stats für Secrets
    with engine.connect() as conn:
        df_type = pd.read_sql_query(text("""
            SELECT COALESCE(NULLIF(TRIM(lso.file_type), ''), 'unknown') AS file_type,
                   COUNT(*) AS n
            FROM layer_secret_occurrences lso
            GROUP BY 1
            ORDER BY n DESC
        """), conn)
        df_file_stats = pd.read_sql_query(text("""
            SELECT
              AVG(lso.file_size)::double precision AS avg_file_size,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lso.file_size) AS median_file_size
            FROM layer_secret_occurrences lso
            WHERE lso.file_size IS NOT NULL
        """), conn)
        df_layer_stats = pd.read_sql_query(text("""
            SELECT
              AVG(l.max_depth)::double precision AS avg_max_depth,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.max_depth) AS median_max_depth,
              AVG(l.file_count)::double precision AS avg_file_count,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.file_count) AS median_file_count,
              AVG(l.uncompressed_size)::double precision AS avg_uncompressed_size,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.uncompressed_size) AS median_uncompressed_size
            FROM layer_secret_occurrences lso
            JOIN layers l ON l.id = lso.layer_id
        """), conn)

    df_type.to_csv(os.path.join(args.outdir, "secrets_filetype_frequency.csv"), index=False)
    df_file_stats.to_csv(os.path.join(args.outdir, "secrets_file_stats.csv"), index=False)
    df_layer_stats.to_csv(os.path.join(args.outdir, "secrets_layer_stats.csv"), index=False)

    print(f"Outputs written to {args.outdir}")

if __name__ == "__main__":
    main()