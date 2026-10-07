"""CoFi-FaaS cost-time model (paper: "Estimating Processing Time and Monetary Cost").

A query run is a DAG G = (V, E) of stages. For every stage v the measured logs give its resource demand
R = <G, {I_v, W_v, O_v}> (input, compute, output demand) and its allocation A_v = <p_v, c_v, m_v>
(functions, vCPU and GB per function). The model projects the demand with platform parameters kappa and
prices pi onto the processing time T and the monetary cost C:

    T^I_{v,i} = sum_s D^in_{v,i,s} / kappa^bw_s + Q^in_{v,i,s} kappa^lat_s           input time of function i
    T^W_{v,i} = W_{v,i} / (c_v kappa^cpu)                                            compute time
    T^O_{v,i} = sum_s D^out_{v,i,s} / kappa^bw_s + Q^out_{v,i,s} kappa^lat_s         output time
    T_v       = max_i (T^I + T^W + T^O) + kappa^disp                                 stage time
    T         = max over source-to-sink paths of sum T_v                             query time
    C^I_v     = sum_s Q^in_{v,s} pi^get_s
    C^W_v     = p_v pi^inv + sum_i d_{v,i} (m_v pi^gbs + c_v pi^cpus)
    C^O_v     = sum_s Q^out_{v,s} pi^put_s + D^out_{v,s} r_{v,s} pi^sto_s            r: retention of the output
    C         = sum_v C^I_v + C^W_v + C^O_v

`Config.paper()` evaluates exactly these equations. `Config.v5()` (the configuration of the reported results) adds
what the BLOOM-FaaS runner does on the Grid'5000 testbed, each as a switch:
  serial      the runner executes stages one after another, so a stage also waits for the previous one and T is
              the sum of the stage times (instead of the longest DAG path);
  waves       a stage runs its p_v functions through max_parallel runner slots, so T_v is the list-scheduling
              makespan of the p_v function times on max_parallel slots;
  contention  functions running at the same time share the storage bandwidth (effective concurrency kappa^con):
              time per byte = 1/kappa^bw + beta * k, k = functions running at once (measured overlap when
              calibrating, min(p_v, max_parallel) when predicting); also for the reads DuckDB does while computing;
  disp_median kappa^disp is the median dispatch delay (the mean is pulled up by rare cold starts).

Demand split used throughout: what a function times explicitly (scan reads D_rx/Q_rx, uploads D_wx/Q_wx, Bloom-filter
files on NFS) is input/output demand; the data DuckDB reads inside its own processing (D_data, Q_impl) is compute
demand, with one processing rate kappa^cpu per (operator endpoint, storage mode).
"""
import os
import gzip
import json
import math
import heapq
import bisect
import itertools
from dataclasses import dataclass, asdict
from datetime import datetime
import numpy as np

CLASSES = ("s3", "nfs")
JOINS = ("broadcast_join", "broadcast_join_pushdown", "hash_join", "range_join", "adaptive_join")
GB, MB = 2 ** 30, 2 ** 20

# Pricing parameters pi (AWS-like list prices, used for both measured and estimated cost)
#
PRICE = {
    "inv": 0.2e-6,                            # per invocation
    "gbs": 16.6667e-6,                        # per GB-second
    "cpus": 0.0,                              # per vCPU-second (memory-only billing)
    "get": {"s3": 0.4e-6, "nfs": 0.0},        # per read request
    "put": {"s3": 5e-6, "nfs": 0.0},          # per write request
    "sto": {"s3": 0.023 / (30 * 24 * 3600), "nfs": 0.30 / (30 * 24 * 3600)},   # per GB-second stored
    "net": 0.0,                               # per GB transferred
}


@dataclass
class Config:
    serial: bool = False
    waves: bool = False
    contention: bool = False
    disp_median: bool = False
    r_hash: float = 5e7                       # hashes per second per vCPU for Bloom-filter build / apply
    cpu: float = 2.0                          # fallback allocation when a log has none
    mem_gb: float = 2.0
    bw0: tuple = (("s3", 100e6), ("nfs", 200e6))   # starting values, replaced by calibration
    lat0: tuple = (("s3", 0.01), ("nfs", 0.001))

    @staticmethod
    def paper():
        """The equations of the paper as written."""
        return Config()

    @staticmethod
    def v5():
        """Configuration of the reported results (cost-report v5)."""
        return Config(serial=True, waves=True, contention=True, disp_median=True)

    def asdict(self):
        return asdict(self)


# ----------------------------------------------------------------------------------------------- measured runs

def ts(x):
    return datetime.fromisoformat(x).timestamp()


def open_stages(folder):
    p = os.path.join(folder, "cost-stages.jsonl")
    return gzip.open(p + ".gz", "rt") if os.path.exists(p + ".gz") else open(p)


def load(folder, cfg=Config()):
    """One run: its DAG and, per stage, the measured demand, allocation and times (`m`)."""
    dag = json.load(open(os.path.join(folder, "cost-dag.json")))
    stages = {}
    with open_stages(folder) as f:
        for line in f:
            r = json.loads(line)
            stages[r["stage"]] = r
    steps = [s for s in dag["steps"] if s["func"] != "summary_clean" and s["name"] in stages]
    overlap([t for s in steps for t in stages[s["name"]]["tasks"]])
    return {"folder": folder, "dag": dag, "steps": steps, "by_name": {s["name"]: s for s in steps},
            "m": {s["name"]: measure(stages[s["name"]], cfg) for s in steps}}


def overlap(tasks):
    """t["k"] = number of functions of the run (itself included) running at some point of this function's life."""
    iv = [(ts(t["start"]), ts(t["end"])) for t in tasks]
    starts, ends = sorted(a for a, _ in iv), sorted(b for _, b in iv)
    for t, (a, b) in zip(tasks, iv):
        t["k"] = max(bisect.bisect_left(starts, b) - bisect.bisect_right(ends, a), 1)


def io(t, side, cls, key):
    return t.get(side, {}).get(cls, {}).get(key, 0)


def data_io(t, side, c):
    """Data I/O of a function in class c without its Bloom-filter files (those live on NFS and are counted apart)."""
    b, q = io(t, side, c, "bytes"), io(t, side, c, "req")
    if c == "nfs":
        bf = t.get("bf_read" if side == "in" else "bf_write") or {"bytes": 0, "n": 0}
        b, q = b - bf["bytes"], q - bf["n"]
    return max(b, 0), max(q, 0)


def k_of(err):
    """Number of hash functions of a Bloom filter with false-positive rate err: k = round(log2(1/err))."""
    return max(1, round(-math.log2(float(err or 0.001))))


def task_bf_work(t):
    """W^{bf-build} = k |B| rows hashed by a builder, W^{bf-apply} = k |P| rows tested by an applier (per function)."""
    bf = t.get("bf") or {}
    k = k_of(bf.get("error_rate"))
    if bf.get("role") == "build":
        return k * (t.get("rows_out") or 0)
    if bf.get("role") == "apply":
        return k * (t.get("rows_in") or 0)
    return 0


def demand(t, endpoint):
    """Demand of one function i of a stage: what it times as input/output, and what DuckDB reads while computing."""
    scan, merge = endpoint.startswith("/scan"), endpoint == "/merge-bf"
    s3i, nfsi = data_io(t, "in", "s3"), data_io(t, "in", "nfs")
    s3o = data_io(t, "out", "s3")
    up = endpoint != "/hash-partition"                    # hash_partition uploads in its own untimed pool
    bfr, bfw = t.get("bf_read") or {"bytes": 0, "n": 0}, t.get("bf_write") or {"bytes": 0, "n": 0}
    return {
        "mode": "nfs" if nfsi[0] + data_io(t, "out", "nfs")[0] > 0 else "s3",
        "D_data": s3i[0] + nfsi[0],                       # bytes processed (compute demand W = D w_op)
        "D_rx": s3i[0] if scan else 0, "Q_rx": s3i[1] if scan else 0,          # D^in, Q^in timed by a scan
        "Q_impl": (nfsi[1] if scan else s3i[1] + nfsi[1]),                    # requests made while computing
        "D_wx": s3o[0] if up else 0, "Q_wx": s3o[1] if up else 0,             # D^out, Q^out (S3 uploads)
        "bf_rx_b": bfr["bytes"] if merge else 0, "bf_rx_n": bfr["n"] if merge else 0,   # merge reads local BFs
        "bf_wx_b": bfw["bytes"] if merge else 0, "bf_wx_n": bfw["n"] if merge else 0,   # merge writes the global BF
        "bf_impl_b": 0 if merge else bfr["bytes"] + bfw["bytes"], "bf_impl_n": 0 if merge else bfr["n"] + bfw["n"],
        "W_bf": task_bf_work(t),
        "W_merge": 8 * bfr["bytes"] if merge else 0,
    }


def measure(r, cfg):
    """Stage totals: demand summed over its p_v functions, allocation, and the measured times."""
    tk = r["tasks"]
    m = {
        "p": len(tk), "cpu": tk[0].get("cpu") or cfg.cpu, "mem_gb": (tk[0].get("mem") or cfg.mem_gb * GB) / GB,
        "endpoint": tk[0].get("endpoint"), "tasks": tk,
        "d_fn": max(t["t_total"] + t["disp"] for t in tk), "d_wall": r["wall_s"],
        "billed": sum(t["t_total"] for t in tk), "disp": sum(t["disp"] for t in tk) / len(tk),
        "t_read": sum(t["t_read"] for t in tk) / len(tk), "t_comp": sum(t["t_comp"] for t in tk) / len(tk),
        "t_write": sum(t["t_write"] for t in tk) / len(tk),
        "rows_in": sum(t.get("rows_in", 0) for t in tk), "rows_out": sum(t.get("rows_out", 0) for t in tk),
        "cold": sum(1 for t in tk if t.get("cold")),
        "start": min(ts(t["start"]) for t in tk), "end": max(ts(t["end"]) for t in tk),
        "bf": next((t["bf"] for t in tk if t.get("bf")), None),
        "bf_read_n": sum(t["bf_read"]["n"] for t in tk), "bf_read_b": sum(t["bf_read"]["bytes"] for t in tk),
        "bf_write_n": sum(t["bf_write"]["n"] for t in tk), "bf_write_b": sum(t["bf_write"]["bytes"] for t in tk),
    }
    for c in CLASSES:
        for side in ("in", "out"):
            m[f"D_{side}_{c}"] = sum(io(t, side, c, "bytes") for t in tk)
            m[f"Q_{side}_{c}"] = sum(io(t, side, c, "req") for t in tk)
    m["D_in"] = sum(m[f"D_in_{c}"] for c in CLASSES)
    m["D_out"] = sum(m[f"D_out_{c}"] for c in CLASSES)
    f = [demand(t, m["endpoint"]) for t in tk]
    for k in f[0]:
        if k != "mode":
            m[k] = sum(x[k] for x in f)
    m["mode"] = f[0]["mode"]
    return m


# ------------------------------------------------------------------------------------- calibration of kappa

def lstsq2(points):
    a11 = sum(d * d for d, q, t in points)
    a12 = sum(d * q for d, q, t in points)
    a22 = sum(q * q for d, q, t in points)
    b1 = sum(d * t for d, q, t in points)
    b2 = sum(q * t for d, q, t in points)
    det = a11 * a22 - a12 * a12
    if not points or abs(det) < 1e-12:
        return None
    return (b1 * a22 - b2 * a12) / det, (a11 * b2 - a12 * b1) / det


def nnls(rows, y):
    """Exact non-negative least squares for a handful of features (best fit over all supports)."""
    A, b = np.array(rows, float), np.array(y, float)
    n, best, best_err = A.shape[1], np.zeros(A.shape[1]), float(b @ b)
    for r in range(1, n + 1):
        for cols in itertools.combinations(range(n), r):
            x = np.linalg.lstsq(A[:, cols], b, rcond=None)[0]
            if (x >= 0).all():
                full = np.zeros(n)
                full[list(cols)] = x
                err = float(((A @ full - b) ** 2).sum())
                if err < best_err:
                    best, best_err = full, err
    return [float(v) for v in best]


def io_time(K, d, q, cls, k=1, beta=None):
    """d / kappa^bw_s + q kappa^lat_s (+ beta_s d k under contention)."""
    return d / K["bw"][cls] + q * K["lat"][cls] + (d * beta.get(cls, 0.0) * k if beta else 0.0)


def calibrate(runs, cfg):
    """Platform parameters kappa from measured runs (in the paper's evaluation: all runs of the other queries).
    kappa^bw_s, kappa^lat_s: S3 from scan reads and uploads, NFS from the Bloom-filter files of merge_bf;
    kappa^cpu per (endpoint, mode): t_comp = D_data / (c kappa^cpu) + Q_impl kappa^qlat, the DuckDB reads included;
    kappa^disp: dispatch delay."""
    K = {"bw": dict(cfg.bw0), "lat": dict(cfg.lat0), "beta": {}, "rk": {}}
    tasks = [(m, t, demand(t, m["endpoint"])) for r in runs for m in r["m"].values() for t in m["tasks"]]
    for c in CLASSES:
        if cfg.contention and c == "s3":
            rows, y = [], []
            for m, t, f in tasks:
                for d, q, tt in ((f["D_rx"], f["Q_rx"], t["t_read"]), (f["D_wx"], f["Q_wx"], t["t_write"])):
                    if tt > 0 and d > 0:
                        rows.append((d, d * t["k"], q))
                        y.append(tt)
            if rows:
                a, K["beta"][c], K["lat"][c] = nnls(rows, y)
                K["bw"][c] = 1 / a if a > 0 else 1e15
            continue
        pts = []
        for m, t, f in tasks:
            if c == "s3":
                if t["t_read"] > 0 and f["D_rx"] > 0:
                    pts.append((f["D_rx"], f["Q_rx"], t["t_read"]))
                if t["t_write"] > 0 and f["D_wx"] > 0:
                    pts.append((f["D_wx"], f["Q_wx"], t["t_write"]))
            else:
                if t["t_read"] > 0 and f["bf_rx_b"] > 0:
                    pts.append((f["bf_rx_b"], f["bf_rx_n"], t["t_read"]))
                if t["t_write"] > 0 and f["bf_wx_b"] > 0:
                    pts.append((f["bf_wx_b"], f["bf_wx_n"], t["t_write"]))
        if pts:
            fit = lstsq2(pts)
            if fit and fit[0] > 0 and fit[1] >= 0:
                K["bw"][c], K["lat"][c] = 1 / fit[0], fit[1]
            else:
                d, t = sum(p[0] for p in pts), sum(p[2] for p in pts)
                if d > 0 and t > 0:
                    K["bw"][c], K["lat"][c] = d / t, 0.0
    groups, disp = {}, []
    for m, t, f in tasks:
        y = t["t_comp"] - f["W_bf"] / (m["cpu"] * cfg.r_hash) - io_time(K, f["bf_impl_b"], f["bf_impl_n"], "nfs")
        if f["D_data"] > 0:
            groups.setdefault(f"{m['endpoint']}@{f['mode']}", []).append(
                (f["D_data"] / m["cpu"], f["Q_impl"], max(y, 1e-3), f["D_data"] * t["k"]))
        disp.append(t["disp"])
    K["rate"], K["qlat"] = {}, {}
    for key, pts in groups.items():
        if cfg.contention:
            a, K["rk"][key], K["qlat"][key] = nnls([(d, dk, q) for d, q, _, dk in pts], [t for _, _, t, _ in pts])
            K["rate"][key] = 1 / a if a > 0 else 1e15
            continue
        pts = [x[:3] for x in pts]
        fit = lstsq2(pts)
        if fit and fit[0] > 0 and fit[1] >= 0:
            K["rate"][key], K["qlat"][key] = 1 / fit[0], fit[1]
        else:
            K["rate"][key], K["qlat"][key] = sum(p[0] for p in pts) / sum(p[2] for p in pts), 0.0
    K["disp"] = (sorted(disp)[len(disp) // 2] if cfg.disp_median else sum(disp) / len(disp)) if disp else 0.0
    K["r_hash"] = cfg.r_hash
    return K


# --------------------------------------------------------------------------------------------- projection

def preds(run, s, serial):
    """Stages that must finish before s starts: its DAG deps, plus the previous stage when the runner is serial."""
    i = run["steps"].index(s)
    return list(s["deps"]) + ([run["steps"][i - 1]["name"]] if serial and i else [])


def makespan(durations, slots):
    """List scheduling of the function times, in submission order, on `slots` runner workers."""
    if not slots or len(durations) <= slots:
        return max(durations, default=0.0)
    free = [0.0] * slots
    for d in durations:
        heapq.heappush(free, heapq.heappop(free) + d)
    return max(free)


def rate_key(K, m):
    for mode in (m["mode"], "s3" if m["mode"] == "nfs" else "nfs"):
        key = f"{m['endpoint']}@{mode}"
        if key in K["rate"]:
            return key
    return None


def stage_mp(run, s):
    return (s or {}).get("max_parallel") or run["dag"].get("max_parallel")


def predict(run, K, cfg):
    """Per stage: T^I, T^W (incl. Bloom-filter work), T^O of one function (demand / p_v), stage time T_v, and the
    billed function-seconds sum_i d_{v,i}."""
    pred = {}
    for s in run["steps"]:
        m = run["m"][s["name"]]
        p, c = m["p"], m["cpu"]
        mp = stage_mp(run, s) or p
        k, beta = (min(p, mp), K.get("beta")) if cfg.contention else (1, None)
        t_in = io_time(K, m["D_rx"] / p, m["Q_rx"] / p, "s3", k, beta) + \
            io_time(K, m["bf_rx_b"] / p, m["bf_rx_n"] / p, "nfs")
        t_out = io_time(K, m["D_wx"] / p, m["Q_wx"] / p, "s3", k, beta) + \
            io_time(K, m["bf_wx_b"] / p, m["bf_wx_n"] / p, "nfs")
        key = rate_key(K, m)
        t_w = (m["D_data"] / p) / (c * K["rate"][key]) + (m["Q_impl"] / p) * K["qlat"][key] if key else 0.0
        if key and beta is not None:
            t_w += (m["D_data"] / p) * K["rk"].get(key, 0.0) * k
        t_bf = m["W_bf"] / p / (c * K["r_hash"]) + io_time(K, m["bf_impl_b"] / p, m["bf_impl_n"] / p, "nfs")
        d = t_in + t_w + t_bf + t_out + K["disp"]
        pred[s["name"]] = {"t_in": t_in, "t_c": t_w + t_bf, "t_bf": t_bf, "t_out": t_out, "d": d, "billed": p * d}
        if cfg.waves:
            pred[s["name"]]["d"] = makespan([d] * p, stage_mp(run, s))
    return pred


def critical_path(run, dur, serial=False):
    """Earliest finish time of every stage; T = the latest one (longest path, or the sum when serial)."""
    eft = {}
    for s in run["steps"]:
        eft[s["name"]] = max([eft[d] for d in preds(run, s, serial) if d in eft] or [0.0]) + dur[s["name"]]
    return max(eft.values()), eft


def cost(m, billed, t_ret, price=PRICE):
    """(C^I_v, C^W_v, C^O_v) of a stage, with its billed function-seconds and the retention of its output."""
    ci = sum(m[f"Q_in_{c}"] * price["get"][c] for c in CLASSES)
    cc = m["p"] * price["inv"] + billed * (m["mem_gb"] * price["gbs"] + m["cpu"] * price["cpus"])
    co = sum(m[f"Q_out_{c}"] * price["put"][c] + m[f"D_out_{c}"] / GB * t_ret * price["sto"][c] for c in CLASSES)
    return ci, cc, co


def pct(model, meas):
    return (model - meas) / meas * 100 if meas else float("nan")


def evaluate(run, K, cfg, price=PRICE):
    """Model vs measurement for one run: per-stage rows and query totals (T, C)."""
    pred = predict(run, K, cfg)
    T_model, eft = critical_path(run, {n: v["d"] for n, v in pred.items()}, cfg.serial)
    q_start = min(m["start"] for m in run["m"].values())
    q_end = max(m["end"] for m in run["m"].values())
    rows = []
    for s in run["steps"]:
        n, m, pv = s["name"], run["m"][s["name"]], pred[s["name"]]
        cm = cost(m, pv["billed"], T_model - eft[n], price)          # retention until the query ends
        cx = cost(m, m["billed"], q_end - m["end"], price)
        rows.append({"stage": n, "func": s["func"], "p": m["p"],
                     "meas_t_read": m["t_read"], "model_t_in": pv["t_in"], "meas_t_comp": m["t_comp"],
                     "model_t_c": pv["t_c"], "meas_t_write": m["t_write"], "model_t_out": pv["t_out"],
                     "meas_d_fn": m["d_fn"], "meas_d_wall": m["d_wall"], "model_d": pv["d"],
                     "meas_C": sum(cx), "model_C": sum(cm), "err_C_%": pct(sum(cm), sum(cx))})
    net = sum(m["D_in_s3"] + m["D_out_s3"] for m in run["m"].values()) / GB * price["net"]
    tot = {"T_meas": q_end - q_start, "T_model": T_model, "err_T_%": pct(T_model, q_end - q_start),
           "C_meas": sum(r["meas_C"] for r in rows) + net, "C_model": sum(r["model_C"] for r in rows) + net}
    tot["err_C_%"] = pct(tot["C_model"], tot["C_meas"])
    return rows, tot
