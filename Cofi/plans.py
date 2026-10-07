import copy
import gzip
import json
import math
import os
import statistics

from . import model as cm
from .bloom import ancestors, descendants, semijoin
from .partition import partition_tasks

MB = 2 ** 20
DEFAULT_MB = 50
DEFAULT_MP = 70
LAMBDAS = [i / 10 for i in range(11)]
DATA = ["D_data", "D_rx", "Q_rx", "Q_impl", "D_wx", "Q_wx", "rows_in", "rows_out", "D_in", "D_out"] + \
    [f"{x}_{s}_{c}" for x in ("D", "Q") for s in ("in", "out") for c in cm.CLASSES]
BFF = ["bf_rx_b", "bf_rx_n", "bf_wx_b", "bf_wx_n", "bf_impl_b", "bf_impl_n", "W_bf", "W_merge"]


def bloom_filters(bf, nobf):
    """One entry per merge_bf stage: builders, appliers, region (stages whose data changes), measured pass rate."""
    out = []
    for s in bf["steps"]:
        if s["func"] != "merge_bf":
            continue
        builders = [d for d in s["deps"] if d in bf["m"]]
        appliers = [a["name"] for a in bf["steps"] if s["name"] in a["deps"] and a["params"].get("filter_by_bf")]
        region, phi = set(), []
        for a in appliers:
            J, _ = semijoin(nobf, builders, a)
            down = descendants(nobf, a)
            region |= {a} | (down & ancestors(nobf, J["name"]) if J else down)
            n0, n1 = nobf["m"][a]["rows_out"], bf["m"][a]["rows_out"]
            phi.append(n1 / n0 if n0 else 1.0)
        out.append({"name": s["name"], "builders": builders, "appliers": appliers,
                    "region": region, "phi": math.prod(phi) if phi else 1.0})
    return out


def synth(bf, nobf, bfs, on):
    """A run for the plan where exactly the BFs in `on` (names of merge_bf stages) are pushed down."""
    off = {j["name"] for j in bfs} - set(on)
    steps, M = [], {}
    for s in bf["steps"]:
        n = s["name"]
        if n in off:
            continue
        cover = [j for j in bfs if n in j["region"]]
        on_c = [j for j in cover if j["name"] in on]
        if n not in nobf["m"] or (len(on_c) == len(cover) and cover):
            m = copy.copy(bf["m"][n])
        else:
            m = copy.copy(nobf["m"][n])
            if on_c:
                f = math.prod(j["phi"] for j in on_c)
                for k in DATA:
                    m[k] = m[k] * f
                m["p"] = max(math.ceil(m["p"] * f), bf["m"][n]["p"])
        own = [j for j in bfs if n in j["builders"] or n in j["appliers"]]
        b = bf["m"][n]
        keep = n not in nobf["m"] or any(j["name"] in on for j in own)
        for side, rk in (("in", "bf_read"), ("out", "bf_write")):   # BF files are counted in the NFS totals
            m[f"D_{side}_nfs"] += (b[f"{rk}_b"] if keep else 0) - m[f"{rk}_b"]
            m[f"Q_{side}_nfs"] += (b[f"{rk}_n"] if keep else 0) - m[f"{rk}_n"]
        for k in BFF:
            m[k] = b[k] if keep else 0
        m["bf"] = b["bf"] if keep else None
        M[n] = m
        steps.append({**s, "deps": [d for d in s["deps"] if d not in off]})
    return {"folder": bf["folder"], "dag": bf["dag"], "steps": steps, "by_name": {s["name"]: s for s in steps}, "m": M}


def estimate(run, K, cfg):
    pred = cm.predict(run, K, cfg)
    T, eft = cm.critical_path(run, {n: v["d"] for n, v in pred.items()}, cfg.serial)
    C = sum(sum(cm.cost(run["m"][n], pred[n]["billed"], T - eft[n])) for n in pred)
    return T, C


def pareto(rows):
    return [r for r in rows if not any(o["T"] <= r["T"] and o["C"] <= r["C"] and (o["T"], o["C"]) != (r["T"], r["C"])
                                       for o in rows)]


# Functions per stage

def load_pipeline(path):
    return {s["name"]: s for s in json.load(open(path))["steps"]}


def load_catalog(path):
    with (gzip.open(path, "rt") if path.endswith(".gz") else open(path)) as f:
        return json.load(f)


class Sizer:
    """p_v for a given max_size_mb, mirroring the runner (pipeline: data/pipelines/<q>.json, catalog: bucket metadata)."""

    def __init__(self, steps, catalog):
        self.steps = steps
        self.files = {d["path"].rstrip("/"): d["files"] for d in catalog["datasets"].values()}

    def scan(self, name, x):
        s = self.steps[name]
        files = self.files.get(str(s["inputs"]).rstrip("/"), [])
        return sum(len(partition_tasks([f], max_size_mb=x, split_row_groups=True, columns=s.get("columns"),
                                       single_file=bool(s.get("single_row_group_per_task")))) for f in files)

    @staticmethod
    def groups(n_files, total_mb, x):
        if n_files <= 0:
            return 1
        each = total_mb / n_files
        return len(partition_tasks([{"full_path": f"f{i}", "size_mb": each} for i in range(n_files)], max_size_mb=x))

    def default(self, name):
        return self.steps[name].get("max_size_mb") or DEFAULT_MB

    def tunable(self, run):
        return [s["name"] for s in run["steps"] if s["func"] in ("scan", "aggregate", "broadcast_join")
                and s["name"] in self.steps and not (s["func"] == "aggregate" and self.steps[s["name"]]["func"] != "aggregate")]

    def p_of(self, run, name, x, P_new):
        """New p_v of stage `name` at max_size_mb = x, given the new p of its upstream stages (P_new)."""
        s, m = run["by_name"][name], run["m"][name]
        if s["func"] == "scan":
            return self.scan(name, x)
        data_deps = [d for d in s["deps"] if run["by_name"].get(d, {}).get("func") != "merge_bf"]
        if s["func"] == "aggregate" and data_deps:
            d = data_deps[0]
            return self.groups(P_new.get(d, run["m"][d]["p"]), run["m"][d]["D_out"] / MB, x)
        if s["func"] == "broadcast_join" and len(data_deps) >= 2:
            b, pr = data_deps[0], data_deps[1]
            return (self.groups(P_new.get(b, run["m"][b]["p"]), run["m"][b]["D_out"] / MB, x) *
                    self.groups(P_new.get(pr, run["m"][pr]["p"]), run["m"][pr]["D_out"] / MB, x))
        return m["p"]


def resize(run, sizer, X):
    """Copy of `run` with max_size_mb X[stage] (and max_parallel X["mp:<stage>"]) applied; returns (run, {stage: p},
    largest input of one function in MB)."""
    out = {**run, "m": {}}
    P_new = {}
    tun = set(sizer.tunable(run))
    for s in run["steps"]:
        n, m0 = s["name"], run["m"][s["name"]]
        m = dict(m0)
        p0 = m0["p"]
        if n in tun:
            x0, x = sizer.default(n), X.get(n, sizer.default(n))
            sim0 = sizer.p_of(run, n, x0, {})
            sim = sizer.p_of(run, n, x, P_new)
            p = max(1, round(p0 * sim / sim0)) if sim0 else p0   # keep the measured p at the default size
        else:
            p = p0
        P_new[n] = p
        r = p / p0 if p0 else 1.0
        data_deps = [d for d in s["deps"] if run["by_name"].get(d, {}).get("func") != "merge_bf"]
        fin = [P_new[d] / run["m"][d]["p"] for d in data_deps if d in P_new and run["m"][d]["p"]]
        rin = statistics.mean(fin) if fin else 1.0        # change in the number of input files
        if s["func"] == "scan":                           # footer + one range GET per column chunk
            dq = 2 * (p - p0)
            m["Q_rx"] = max(m["Q_rx"] + dq, p)
            m["Q_in_s3"] = max(m["Q_in_s3"] + dq, p)
        else:
            m["Q_impl"] *= rin
            m["Q_in_s3"] *= rin
        m["Q_wx"] *= r                                     # one output file per function
        m["Q_out_s3"] *= r
        m["bf_impl_b"] *= r                                # each function reads / writes its own BF file
        m["bf_impl_n"] *= r
        if s["func"] == "merge_bf":
            m["bf_rx_b"] *= rin
            m["bf_rx_n"] *= rin
            m["Q_in_nfs"] *= rin
            m["D_in_nfs"] *= rin
        m["p"] = p
        out["m"][n] = m
    load = max(m["D_data"] / m["p"] / MB for m in out["m"].values())
    mp = {k[3:]: v for k, v in X.items() if k.startswith("mp:")}
    if mp:
        out["steps"] = [dict(s, max_parallel=mp[s["name"]]) if s["name"] in mp else s for s in run["steps"]]
        out["by_name"] = {s["name"]: s for s in out["steps"]}
    return out, P_new, load

class Query:
    """Plan search of one query. data: {query: {system: [runs]}} (cofi.accuracy.load_history)."""

    def __init__(self, q, data, cfg, sizer, sizes, mps=(), max_fn_mb=200, seeds=5):
        R = data[q]
        self.q, self.cfg, self.sizer, self.sizes, self.mps, self.max_fn_mb = q, cfg, sizer, sizes, list(mps), max_fn_mb
        self.K = cm.calibrate([r for q2, R2 in data.items() if q2 != q for rs in R2.values() for r in rs], cfg)  # LOQO
        pairs = list(zip(R["s3"], R["base"]))
        step = max(1, len(pairs) // seeds)
        self.pairs = pairs[::step]                         # one BF / no-BF run pair per seed
        self.bfs = bloom_filters(*self.pairs[0])
        self.names = [j["name"] for j in self.bfs]
        self.cache, self.base_cache = {}, {}
        self.tun = sizer.tunable(synth(*self.pairs[0], self.bfs, self.names))

    def dflt(self, k):
        return DEFAULT_MP if k.startswith("mp:") else self.sizer.default(k)

    def plan_key(self, on, X):
        return tuple(sorted(on)), tuple(sorted((k, v) for k, v in X.items() if v != self.dflt(k)))

    def eval(self, on, X):
        key = self.plan_key(on, X)
        if key not in self.cache:
            est, ps, load = [], None, 0.0
            for i, (bf, nobf) in enumerate(self.pairs):
                bk = (i, tuple(sorted(on)))
                if bk not in self.base_cache:
                    self.base_cache[bk] = synth(bf, nobf, bloom_filters(bf, nobf), list(on))
                run, ps, ld = resize(self.base_cache[bk], self.sizer, X)
                load = max(load, ld)
                est.append(estimate(run, self.K, self.cfg))
            self.cache[key] = {"on": sorted(on), "X": {k: v for k, v in X.items() if v != self.dflt(k)},
                               "p": ps, "T": statistics.median(t for t, _ in est),
                               "C": statistics.median(c for _, c in est), "max_fn_mb": load,
                               "ok": load <= self.max_fn_mb or not key[1]}
        return self.cache[key]

    def descend(self, lam, T0, C0, on, X, passes=4):
        f = lambda r: lam * r["T"] / T0 + (1 - lam) * r["C"] / C0 if r["ok"] else math.inf
        on, X = set(on), dict(X)
        best = self.eval(on, X)
        for _ in range(passes):
            improved = False
            for j in self.names:
                cand = on ^ {j}
                r = self.eval(cand, X)
                if f(r) < f(best) - 1e-12:
                    on, best, improved = cand, r, True
            for n in self.tun:
                for x in self.sizes:
                    cand = {**X, n: x}
                    r = self.eval(on, cand)
                    if f(r) < f(best) - 1e-12:
                        X, best, improved = cand, r, True
                for mp in self.mps:
                    cand = {**X, f"mp:{n}": mp}
                    r = self.eval(on, cand)
                    if f(r) < f(best) - 1e-12:
                        X, best, improved = cand, r, True
            if not improved:
                break
        return best

    def optimize(self):
        default = self.eval(self.names, {})
        T0, C0 = default["T"], default["C"]
        for lam in LAMBDAS:
            self.descend(lam, T0, C0, self.names, {})
            self.descend(lam, T0, C0, [], {})
        allp = [r for r in self.cache.values() if r["ok"]]
        front = sorted({(r["T"], r["C"]): r for r in pareto(allp)}.values(), key=lambda r: r["T"])
        tmin, cmin = min(r["T"] for r in front), min(r["C"] for r in front)
        knee = min(front, key=lambda r: math.hypot(r["T"] / tmin - 1, r["C"] / cmin - 1))
        return {"default": default, "knee": knee, "min_T": front[0], "min_C": front[-1], "front": front, "all": allp}


def plan_json(q, name, r, names):
    return {"query": q, "plan": name, "T_est": r["T"], "C_est": r["C"], "max_fn_mb": r["max_fn_mb"],
            "bf_off": [n for n in names if n not in r["on"]],
            "max_size_mb": {k: v for k, v in r["X"].items() if not k.startswith("mp:")},
            "max_parallel": {k[3:]: v for k, v in r["X"].items() if k.startswith("mp:")}, "p": r["p"]}


def run_query(q, data, cfg, pipeline_dir, catalog, sizes, mps=(), max_fn_mb=200):
    """Optimize one query: plans (default / knee / min_T / min_C), every evaluated point, and the p_v check."""
    sizer = Sizer(load_pipeline(os.path.join(pipeline_dir, f"{q}.json")), catalog)
    Q = Query(q, data, cfg, sizer, sizes, mps, max_fn_mb)
    bf = Q.pairs[0][0]
    check = [{"query": q, "stage": n, "p_meas": bf["m"][n]["p"], "p_sim": sizer.p_of(bf, n, sizer.default(n), {})}
             for n in sizer.tunable(bf)]
    res = Q.optimize()
    plans = [plan_json(q, name, res[name], Q.names) for name in ("default", "knee", "min_T", "min_C")]
    points = [{"query": q, "T": r["T"], "C": r["C"], "pareto": r in res["front"], "n_on": len(r["on"]),
               "n_resized": len(r["X"]), "knee": r is res["knee"], "default": r is res["default"]} for r in res["all"]]
    return plans, points, check
