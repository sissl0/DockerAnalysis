import os
import sys
import json
import math
import argparse
from collections import Counter
from datetime import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from scipy.stats import ks_2samp, chisquare

# ---------- args ----------

def parse_args():
    p = argparse.ArgumentParser(description="Validate sample vs. full dataset (bias, KS/Chi² tests).")
    p.add_argument("--digest-analysis", required=True, help="Path to digest_analysis.jsonl (sample)")
    p.add_argument("--unique-repos", help="Path to unique_repos.jsonl (full; fallback if no precomputed CSV)")
    p.add_argument("--combined-tags", help="Path to combined_tags.jsonl (full; fallback if no precomputed CSV)")
    p.add_argument("--db-url", default=os.environ.get("DATABASE_URL", ""), help="Postgres URL for sample DB")
    p.add_argument("--outdir", default="analysis_outputs", help="Output directory for CSVs")
    p.add_argument("--precomputed-dir", default="", help="Directory containing precomputed CSVs from Go")
    p.add_argument("--max-ks-samples", type=int, default=500000, help="Max reservoir sample size for JSONL fallback")
    return p.parse_args()

# ---------- helpers ----------

def ensure_outdir(p):
    os.makedirs(p, exist_ok=True)

def month_str(ts: pd.Timestamp) -> str:
    ts = pd.to_datetime(ts)
    return f"{ts.year:04d}-{ts.month:02d}"

def read_hist_csv(path: str) -> Counter:
    df = pd.read_csv(path)
    if df.empty or "key" not in df or "count" not in df:
        return Counter()
    return Counter({str(k): int(v) for k, v in zip(df["key"], df["count"])})

def read_series_csv(path: str) -> np.ndarray:
    df = pd.read_csv(path)
    if "value" not in df or df.empty:
        return np.array([], dtype=float)
    return df["value"].astype(float).to_numpy()

def normalize_cat(s):
    if s is None:
        return "unknown"
    s = str(s).strip()
    return s if s else "unknown"

def make_bias_table(full_counts: Counter, sample_counts: Counter) -> pd.DataFrame:
    keys = sorted(set(full_counts.keys()) | set(sample_counts.keys()))
    rows = []
    n_full = sum(full_counts.values())
    n_sam = sum(sample_counts.values())
    for k in keys:
        cf = full_counts.get(k, 0)
        cs = sample_counts.get(k, 0)
        pf = cf / n_full if n_full > 0 else 0.0
        ps = cs / n_sam if n_sam > 0 else 0.0
        bias = (pf / ps) if ps > 0 else (np.inf if pf > 0 else 1.0)
        rows.append({"key": k, "count_full": cf, "count_sample": cs, "p_full": pf, "p_sample": ps, "bias_full_over_sample": bias})
    return pd.DataFrame(rows)

def make_numeric_bias_bins(full_vals: np.ndarray, sample_vals: np.ndarray, nbins: int = 20) -> pd.DataFrame:
    full_vals = full_vals[~np.isnan(full_vals)]
    sample_vals = sample_vals[~np.isnan(sample_vals)]
    if len(full_vals) == 0 or len(sample_vals) == 0:
        return pd.DataFrame(columns=["bin_left","bin_right","count_full","count_sample","p_full","p_sample","bias_full_over_sample"])
    qs = np.linspace(0, 1, nbins + 1)
    edges = np.quantile(full_vals, qs)
    edges = np.unique(edges)
    if len(edges) < 3:
        edges = np.linspace(np.nanmin(full_vals), np.nanmax(full_vals), nbins + 1)
    cf, _ = np.histogram(full_vals, bins=edges)
    cs, _ = np.histogram(sample_vals, bins=edges)
    n_full, n_sam = cf.sum(), cs.sum()
    rows = []
    for i in range(len(edges) - 1):
        pf = cf[i] / n_full if n_full > 0 else 0.0
        ps = cs[i] / n_sam if n_sam > 0 else 0.0
        bias = (pf / ps) if ps > 0 else (np.inf if pf > 0 else 1.0)
        rows.append({
            "bin_left": edges[i],
            "bin_right": edges[i+1],
            "count_full": int(cf[i]),
            "count_sample": int(cs[i]),
            "p_full": pf,
            "p_sample": ps,
            "bias_full_over_sample": bias,
        })
    return pd.DataFrame(rows)

def save_series_counts(counts: Counter, out_path: str):
    total = sum(counts.values())
    rows = []
    for key, n in sorted(counts.items(), key=lambda kv: kv[0]):
        p = n / total if total > 0 else 0.0
        rows.append({"key": key, "count": n, "proportion": p})
    pd.DataFrame(rows).to_csv(out_path, index=False)

def compute_unique_layer_share_from_digest(digest_analysis_path):
    total = 0
    uniq = 0
    with open(digest_analysis_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            total += 1
            rc = obj.get("repo_count")
            if rc is None:
                repos = obj.get("repos") or []
                rc = len(repos)
            if rc == 1:
                uniq += 1
    share = (uniq / total) if total > 0 else math.nan
    return total, uniq, share

# ---------- FULL (prefer precomputed CSVs) ----------

def load_full_tags(pre_dir: str, combined_tags_path: str, max_ks_samples: int):
    if pre_dir:
        lp_month = read_hist_csv(os.path.join(pre_dir, "full_last_pushed_month.csv"))
        status = read_hist_csv(os.path.join(pre_dir, "full_status.csv"))
        lp_epochs = read_series_csv(os.path.join(pre_dir, "full_last_pushed_epoch_reservoir.csv"))
        size = read_series_csv(os.path.join(pre_dir, "full_size_reservoir.csv"))
        if sum(lp_month.values()) > 0 and len(lp_epochs) > 0 and len(size) > 0 and sum(status.values()) > 0:
            return {
                "last_pushed_month_counts": lp_month,
                "last_pushed_epochs_sample": lp_epochs.astype(float),
                "size_sample": size.astype(float),
                "status_counts": status,
            }
        # else: fall through to JSONL
    # JSONL fallback (slow)
    last_pushed_month_counts = Counter()
    last_pushed_reservoir = []
    size_reservoir = []
    status_counts = Counter()
    n_last = 0
    n_size = 0
    rng = np.random.default_rng(42)
    with open(combined_tags_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            lp = obj.get("last_pushed")
            if lp:
                ts = pd.to_datetime(lp, utc=True, errors="coerce")
                if pd.notna(ts):
                    last_pushed_month_counts[month_str(ts)] += 1
                    # reservoir
                    if n_last < max_ks_samples:
                        last_pushed_reservoir.append(ts.value / 1e9)
                        n_last += 1
                    else:
                        j = rng.integers(0, n_last + 1)
                        if j < max_ks_samples:
                            last_pushed_reservoir[j] = ts.value / 1e9
                        n_last += 1
            sz = obj.get("size", None)
            if sz is not None:
                try:
                    val = float(sz)
                    if n_size < max_ks_samples:
                        size_reservoir.append(val)
                        n_size += 1
                    else:
                        j = rng.integers(0, n_size + 1)
                        if j < max_ks_samples:
                            size_reservoir[j] = val
                        n_size += 1
                except Exception:
                    pass
            status = normalize_cat(obj.get("status"))
            status_counts[status] += 1
    return {
        "last_pushed_month_counts": last_pushed_month_counts,
        "last_pushed_epochs_sample": np.array(last_pushed_reservoir, dtype=float),
        "size_sample": np.array(size_reservoir, dtype=float),
        "status_counts": status_counts,
    }

def load_full_repos(pre_dir: str, unique_repos_path: str, max_ks_samples: int):
    if pre_dir:
        io_counts = read_hist_csv(os.path.join(pre_dir, "full_is_official.csv"))
        pull = read_series_csv(os.path.join(pre_dir, "full_pull_count_reservoir.csv"))
        if sum(io_counts.values()) > 0 and len(pull) > 0:
            return {
                "pull_count_sample": pull.astype(float),
                "is_official_counts": io_counts,
            }
        # else: fall through
    # JSONL fallback (slow)
    pull_reservoir = []
    n_pull = 0
    is_official_counts = Counter()
    rng = np.random.default_rng(43)
    with open(unique_repos_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            pc = obj.get("pull_count")
            if pc is not None:
                try:
                    val = float(pc)
                    if n_pull < max_ks_samples:
                        pull_reservoir.append(val)
                        n_pull += 1
                    else:
                        j = rng.integers(0, n_pull + 1)
                        if j < max_ks_samples:
                            pull_reservoir[j] = val
                        n_pull += 1
                except Exception:
                    pass
            iso = bool(obj.get("is_official"))
            is_official_counts["official" if iso else "unofficial"] += 1
    return {
        "pull_count_sample": np.array(pull_reservoir, dtype=float),
        "is_official_counts": is_official_counts,
    }

# ---------- SAMPLE (DB) ----------

def load_sample_from_db(db_url):
    engine = create_engine(db_url)
    with engine.connect() as conn:
        # last_pushed
        df_lp = pd.read_sql_query(text("SELECT last_pushed FROM tags WHERE last_pushed IS NOT NULL"), conn)
        # ns -> s
        lp_epochs = pd.to_datetime(df_lp["last_pushed"]).astype("int64").to_numpy(dtype=float) / 1e9
        df_lp["month_str"] = pd.to_datetime(df_lp["last_pushed"]).map(month_str)
        month_counts = Counter(df_lp["month_str"].tolist())

        # size
        df_sz = pd.read_sql_query(text("SELECT size FROM tags WHERE size IS NOT NULL"), conn)
        size_vals = df_sz["size"].astype(float).to_numpy()

        # status
        df_st = pd.read_sql_query(text("SELECT COALESCE(NULLIF(TRIM(status), ''), 'unknown') AS status FROM tags"), conn)
        status_counts = Counter(df_st["status"].tolist())

        # pull_count
        df_pc = pd.read_sql_query(text("SELECT pull_count FROM repositories WHERE pull_count IS NOT NULL"), conn)
        pull_vals = df_pc["pull_count"].astype(float).to_numpy()

        # is_official
        df_io = pd.read_sql_query(text("SELECT CASE WHEN is_official THEN 'official' ELSE 'unofficial' END AS cat FROM repositories"), conn)
        io_counts = Counter(df_io["cat"].tolist())

    return {
        "last_pushed_epochs": lp_epochs,
        "last_pushed_month_counts": month_counts,
        "size_vals": size_vals,
        "status_counts": status_counts,
        "pull_vals": pull_vals,
        "is_official_counts": io_counts,
    }

# ---------- main ----------

def main():
    args = parse_args()
    if not args.db_url:
        print("DATABASE_URL not set and --db-url not given.", file=sys.stderr)
        sys.exit(1)
    ensure_outdir(args.outdir)

    # Load FULL using precomputed CSVs if available
    full_tags = load_full_tags(args.precomputed_dir, args.combined_tags, args.max_ks_samples)
    full_repos = load_full_repos(args.precomputed_dir, args.unique_repos, args.max_ks_samples)

    # Load SAMPLE from DB
    sample = load_sample_from_db(args.db_url)

    # Unique layer share (sample digest_analysis.jsonl)
    total_layers, uniq_layers, uniq_share = compute_unique_layer_share_from_digest(args.digest_analysis)
    print(f"Unique layer share (sample): {uniq_layers}/{total_layers} = {uniq_share:.4f} (compare to full 0.2820)")

    # KS tests
    ks_last = ks_2samp(full_tags["last_pushed_epochs_sample"], sample["last_pushed_epochs"], alternative="two-sided", method="auto")
    ks_pull = ks_2samp(full_repos["pull_count_sample"], sample["pull_vals"], alternative="two-sided", method="auto")
    ks_size = ks_2samp(full_tags["size_sample"], sample["size_vals"], alternative="two-sided", method="auto")
    print(f"KS last_pushed: statistic={ks_last.statistic:.6f} pvalue={ks_last.pvalue:.3e}")
    print(f"KS pull_count:  statistic={ks_pull.statistic:.6f} pvalue={ks_pull.pvalue:.3e}")
    print(f"KS size:        statistic={ks_size.statistic:.6f} pvalue={ks_size.pvalue:.3e}")

    # Chi-squared tests
    st_full, st_sam = full_tags["status_counts"], sample["status_counts"]
    st_keys = sorted(set(st_full.keys()) | set(st_sam.keys()))
    obs = np.array([st_sam.get(k, 0) for k in st_keys], dtype=float)
    exp_prop = np.array([st_full.get(k, 0) for k in st_keys], dtype=float)
    exp_prop = exp_prop / exp_prop.sum() if exp_prop.sum() > 0 else np.zeros_like(exp_prop)
    exp = exp_prop * obs.sum() if obs.sum() > 0 else np.zeros_like(exp_prop)
    chi_status = chisquare(f_obs=obs, f_exp=exp) if exp.sum() > 0 else None

    io_full, io_sam = full_repos["is_official_counts"], sample["is_official_counts"]
    io_keys = sorted(set(io_full.keys()) | set(io_sam.keys()))
    obs_io = np.array([io_sam.get(k, 0) for k in io_keys], dtype=float)
    exp_prop_io = np.array([io_full.get(k, 0) for k in io_keys], dtype=float)
    exp_prop_io = exp_prop_io / exp_prop_io.sum() if exp_prop_io.sum() > 0 else np.zeros_like(exp_prop_io)
    exp_io = exp_prop_io * obs_io.sum() if obs_io.sum() > 0 else np.zeros_like(exp_prop_io)
    chi_io = chisquare(f_obs=obs_io, f_exp=exp_io) if exp_io.sum() > 0 else None

    if chi_status:
        print(f"Chi² status:     chi2={chi_status.statistic:.6f} pvalue={chi_status.pvalue:.3e} df={len(st_keys)-1}")
    else:
        print("Chi² status:     insufficient data")
    if chi_io:
        print(f"Chi² is_official: chi2={chi_io.statistic:.6f} pvalue={chi_io.pvalue:.3e} df={len(io_keys)-1}")
    else:
        print("Chi² is_official: insufficient data")

    # Bias tables and export
    df_month_bias = make_bias_table(full_tags["last_pushed_month_counts"], sample["last_pushed_month_counts"])
    df_month_bias = df_month_bias.sort_values("key")
    df_month_bias.to_csv(os.path.join(args.outdir, "bias_last_pushed_month.csv"), index=False)

    df_bias_pull = make_numeric_bias_bins(full_repos["pull_count_sample"], sample["pull_vals"], nbins=20)
    df_bias_pull.to_csv(os.path.join(args.outdir, "bias_pull_count_bins.csv"), index=False)

    df_bias_size = make_numeric_bias_bins(full_tags["size_sample"], sample["size_vals"], nbins=20)
    df_bias_size.to_csv(os.path.join(args.outdir, "bias_size_bins.csv"), index=False)

    df_bias_status = make_bias_table(full_tags["status_counts"], sample["status_counts"])
    df_bias_status.to_csv(os.path.join(args.outdir, "bias_status.csv"), index=False)

    df_bias_isoff = make_bias_table(full_repos["is_official_counts"], sample["is_official_counts"])
    df_bias_isoff.to_csv(os.path.join(args.outdir, "bias_is_official.csv"), index=False)

    # also save distributions
    save_series_counts(full_tags["last_pushed_month_counts"], os.path.join(args.outdir, "dist_full_last_pushed_month.csv"))
    save_series_counts(sample["last_pushed_month_counts"], os.path.join(args.outdir, "dist_sample_last_pushed_month.csv"))
    save_series_counts(full_tags["status_counts"], os.path.join(args.outdir, "dist_full_status.csv"))
    save_series_counts(sample["status_counts"], os.path.join(args.outdir, "dist_sample_status.csv"))
    save_series_counts(full_repos["is_official_counts"], os.path.join(args.outdir, "dist_full_is_official.csv"))
    save_series_counts(sample["is_official_counts"], os.path.join(args.outdir, "dist_sample_is_official.csv"))

    # numeric distributions: export quantiles
    def quantiles_series(vals, name):
        vals = vals[~np.isnan(vals)]
        if len(vals) == 0:
            return pd.DataFrame(columns=["quantile","value"]).assign(series=name)
        qs = np.linspace(0, 1, 21)
        qv = np.quantile(vals, qs)
        return pd.DataFrame({"quantile": qs, "value": qv}).assign(series=name)

    q_full_pull = quantiles_series(full_repos["pull_count_sample"], "full_pull_count")
    q_sam_pull = quantiles_series(sample["pull_vals"], "sample_pull_count")
    pd.concat([q_full_pull, q_sam_pull]).to_csv(os.path.join(args.outdir, "quantiles_pull_count.csv"), index=False)

    q_full_size = quantiles_series(full_tags["size_sample"], "full_size")
    q_sam_size = quantiles_series(sample["size_vals"], "sample_size")
    pd.concat([q_full_size, q_sam_size]).to_csv(os.path.join(args.outdir, "quantiles_size.csv"), index=False)

    q_full_lp = quantiles_series(full_tags["last_pushed_epochs_sample"], "full_last_pushed_epoch")
    q_sam_lp = quantiles_series(sample["last_pushed_epochs"], "sample_last_pushed_epoch")
    pd.concat([q_full_lp, q_sam_lp]).to_csv(os.path.join(args.outdir, "quantiles_last_pushed_epoch.csv"), index=False)

    # Save unique share
    pd.DataFrame([{
        "total_layers": total_layers,
        "unique_layers": uniq_layers,
        "unique_share_sample": uniq_share,
        "unique_share_full_reference": 0.2820,
        "bias_abs_diff": (uniq_share - 0.2820)
    }]).to_csv(os.path.join(args.outdir, "unique_layer_share.csv"), index=False)

    print(f"Outputs written to {args.outdir}")

if __name__ == "__main__":
    main()