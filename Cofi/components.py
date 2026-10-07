import os
import glob
import argparse
from dataclasses import replace
import pandas as pd
from . import model as cm
from .accuracy import load_history, SYSTEMS, runs, qnum


def stage_rows(run, K, cfg, dataset, query, system, run_id, size=None):
    task_cfg = replace(cfg, waves=False)                    # time of one function: no composition into waves
    pt, ps = cm.predict(run, K, task_cfg), cm.predict(run, K, cfg)
    tot = cm.evaluate(run, K, cfg)[1]
    out = []
    for s in run["steps"]:
        n, m = s["name"], run["m"][s["name"]]
        d = sorted(t["t_total"] + t["disp"] for t in m["tasks"])
        out.append({"dataset": dataset, "query": query, "system": system, "run": run_id, "size_mb": size,
                    "stage": n, "func": s["func"], "endpoint": m["endpoint"], "p": m["p"],
                    "max_parallel": cm.stage_mp(run, s),
                    "task_meas_mean": sum(d) / len(d), "task_meas_median": d[len(d) // 2], "task_model": pt[n]["d"],
                    "stage_meas": m["d_wall"], "stage_model": ps[n]["d"],
                    "T_meas": tot["T_meas"], "T_model": tot["T_model"]})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("history")
    ap.add_argument("--sweep")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    cfg = cm.Config.v5()
    data = load_history(a.history, cfg)
    rows = []
    for q, R in data.items():
        K = cm.calibrate([r for q2, R2 in data.items() if q2 != q for rs in R2.values() for r in rs], cfg)
        for system, rs in R.items():
            for i, run in enumerate(rs, 1):
                rows += stage_rows(run, K, cfg, "accuracy", q, system, i)
        if a.sweep:
            for sd in sorted(glob.glob(os.path.join(a.sweep, "size-*"))):
                qd = os.path.join(sd, q)
                for i, d in enumerate(runs(qd, SYSTEMS["s3"]), 1):
                    rows += stage_rows(cm.load(d, cfg), K, cfg, "sweep", q, "s3", i, int(sd.rsplit("-", 1)[1]))
        print(f"{q}: {len(rows)} stage rows", flush=True)
    df = pd.DataFrame(rows)
    df["_q"] = df["query"].map(qnum)
    df.sort_values(["dataset", "_q", "system", "size_mb", "run"]).drop(columns="_q").to_csv(a.out, index=False)
    print(f"{len(df)} rows -> {a.out}")


if __name__ == "__main__":
    main()
