import os
import re
import glob
import statistics
import pandas as pd
from . import model as cm
from .bloom import bf_report

SYSTEMS = {"s3": "bloomfaas-s3", "nfs": "bloomfaas-nfs", "base": "starling-base"}
LABEL = {"base": "No BF (Starling-based)", "s3": "BLOOM-FaaS S3", "nfs": "BLOOM-FaaS NFS"}

def qnum(q):
    return int(re.sub(r"\D", "", q) or 0)


def runs(qdir, tag):
    return sorted(d for d in glob.glob(os.path.join(qdir, f"*--{tag}*"))
                  if os.path.exists(os.path.join(d, "cost-stages.jsonl.gz")) or os.path.exists(os.path.join(d, "cost-stages.jsonl")))


def load_history(root, cfg=cm.Config(), queries=None):
    """{query: {system: [run, ...]}} for every query folder of `root`."""
    qdirs = sorted((d for d in glob.glob(os.path.join(root, "q*")) if os.path.isdir(d)), key=lambda d: qnum(os.path.basename(d)))
    return {os.path.basename(d).lower(): {s: [cm.load(x, cfg) for x in runs(d, t)] for s, t in SYSTEMS.items()}
            for d in qdirs if not queries or os.path.basename(d).lower() in queries}


def evaluate_all(data, cfg):
    """Leave-one-query-out evaluation. Returns (runs: one row per query/system/run/metric, bf: BF rows, kappa)."""
    rows, bfrows, kappa = [], [], {}
    for q, R in data.items():
        K = cm.calibrate([r for q2, R2 in data.items() if q2 != q for rs in R2.values() for r in rs], cfg)
        kappa[q] = K
        for variant in ("s3", "nfs"):
            for i, (bf, nobf) in enumerate(zip(R[variant], R["base"]), 1):
                t1, t0 = cm.evaluate(bf, K, cfg)[1], cm.evaluate(nobf, K, cfg)[1]
                sides = [(variant, t1)] + ([("base", t0)] if variant == "s3" else [])   # base once, from the s3 pairing
                for system, t in sides:
                    for m in ("T", "C"):
                        rows.append({"query": q, "system": system, "run": i, "metric": m, "meas": t[f"{m}_meas"],
                                     "model": t[f"{m}_model"], "err_%": t[f"err_{m}_%"]})
                better_meas = t1["T_meas"] < t0["T_meas"] and t1["C_meas"] < t0["C_meas"]
                better_model = t1["T_model"] < t0["T_model"] and t1["C_model"] < t0["C_model"]
                rows.append({"query": q, "system": variant, "run": i, "metric": "decision", "meas": float(better_meas),
                             "model": float(better_model), "err_%": float(better_meas != better_model)})
                if variant == "s3":
                    for j in bf_report(bf, nobf):
                        for a in j["appliers"]:
                            bfrows.append({"query": q, "run": i, "bf": j["join"], "applier": a["stage"],
                                           "R_model": j["R_model"], "R_meas": j["R_meas"],
                                           **{k: a.get(k) for k in ("sigma_semi", "phi_meas", "phi_model", "err_phi_%",
                                                                    "err_D_out_%")}})
    df = pd.DataFrame(rows)
    df["abs_err_%"] = df["err_%"].abs()
    return df, pd.DataFrame(bfrows), kappa


def per_query(df):
    """Median over runs of meas / model / signed error, min-max of the error, and MAPE (mean |error|)."""
    acc = df[df.metric.isin(["T", "C"])]
    out = acc.groupby(["query", "system", "metric"]).agg(
        runs=("run", "nunique"), meas=("meas", "median"), model=("model", "median"), err=("err_%", "median"),
        err_min=("err_%", "min"), err_max=("err_%", "max"), mape=("abs_err_%", "mean")).reset_index()
    dec = df[df.metric == "decision"].groupby(["query", "system"]).agg(
        decision_match=("err_%", lambda s: f"{int((s == 0).sum())}/{len(s)}")).reset_index()
    out = out.merge(dec, on=["query", "system"], how="left")
    out["_q"], out["_s"] = out["query"].map(qnum), out["system"].map(list(SYSTEMS).index)
    return out.sort_values(["_q", "_s", "metric"]).drop(columns=["_q", "_s"]).reset_index(drop=True)


def summary(pq):
    """MAPE per system and metric (mean over queries of the per-query MAPE), and over everything."""
    s = pq.pivot_table(index="system", columns="metric", values="mape", aggfunc="mean")
    s.loc["all"] = pq.groupby("metric")["mape"].mean()
    return s.reindex([x for x in list(SYSTEMS) + ["all"] if x in s.index])
