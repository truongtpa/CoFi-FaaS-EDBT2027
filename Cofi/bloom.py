import math
from .model import JOINS, k_of, pct

def ancestors(run, name):
    deps = {x["name"]: x["deps"] for x in run["dag"]["steps"]}
    seen, stack = set(), [name]
    while stack:
        n = stack.pop()
        if n not in seen:
            seen.add(n)
            stack.extend(deps.get(n, []))
    return seen


def descendants(run, name):
    kids = {}
    for s in run["dag"]["steps"]:
        for d in s["deps"]:
            kids.setdefault(d, []).append(s["name"])
    seen, stack = set(), [name]
    while stack:
        n = stack.pop()
        if n not in seen:
            seen.add(n)
            stack.extend(kids.get(n, []))
    return seen


def semijoin(run, builders, applier):
    """First join with the BF build side on one input and the BF-filtered scan on the other.
    Returns (join step, index of the applier-side dep); sigma = rows_out(join) / rows_out(applier-side dep)."""
    for s in run["steps"]:
        if s["func"] not in JOINS or len(s["deps"]) < 2:
            continue
        anc = [ancestors(run, d) for d in s["deps"]]
        for i, a in enumerate(anc):
            if applier in a and any(b in x for x in anc[:i] + anc[i + 1:] for b in builders):
                return s, i
    return None, None


def bf_report(bf, nobf):
    """One row per Bloom filter (merge_bf stage) of the BF run: model vs measured requests, bytes, pass rate."""
    out = []
    for s in bf["steps"]:
        if s["func"] != "merge_bf":
            continue
        M = bf["m"][s["name"]]
        builds = [bf["m"][d] for d in s["deps"] if d in bf["m"]]
        appliers = [a for a in bf["steps"] if s["name"] in a["deps"] and a["params"].get("filter_by_bf")]
        A = [bf["m"][a["name"]] for a in appliers]
        p_b, p_p = sum(b["p"] for b in builds), sum(a["p"] for a in A)
        m_bits = M["bf_write_b"] * 8
        bbf = next((b["bf"] for b in builds if b["bf"]), {}) or {}
        err = bbf.get("error_rate") or 0.001
        k = k_of(err)
        n_act = sum(b["rows_out"] for b in builds)
        f_model = (1 - math.exp(-k * n_act / m_bits)) ** k if m_bits else float("nan")
        R_model = 2 * p_b + 1 + p_p
        R_meas = sum(b["bf_write_n"] for b in builds) + M["bf_read_n"] + M["bf_write_n"] + sum(a["bf_read_n"] for a in A)
        X_meas = sum(b["bf_write_b"] for b in builds) + M["bf_read_b"] + M["bf_write_b"] + sum(a["bf_read_b"] for a in A)
        row = {"join": s["name"], "p_b": p_b, "p_p": p_p, "R_model": R_model, "R_meas": R_meas,
               "m_bits": m_bits, "k": k, "n_est": float(bbf.get("est_elements") or 0), "n_act": n_act,
               "f_design": err, "f_model": f_model, "X_model": m_bits / 8 * R_model, "X_meas": X_meas,
               "W_build": k * n_act, "W_apply": k * sum(a["rows_in"] for a in A), "appliers": []}
        for a in appliers:
            ab, an = bf["m"][a["name"]], (nobf["m"].get(a["name"]) if nobf else None)
            item = {"stage": a["name"], "rows_in": ab["rows_in"], "rows_out_bf": ab["rows_out"], "D_out_bf": ab["D_out"]}
            if an:
                phi = ab["rows_out"] / an["rows_out"] if an["rows_out"] else float("nan")
                J, side = semijoin(nobf, s["deps"], a["name"])
                Jm = nobf["m"].get(J["name"]) if J else None
                Pm = nobf["m"].get(J["deps"][side]) if J else None
                sigma = min(Jm["rows_out"] / Pm["rows_out"], 1.0) if Jm and Pm and Pm["rows_out"] else float("nan")
                phi_model = sigma + (1 - sigma) * f_model
                item.update({"rows_out_nobf": an["rows_out"], "D_out_nobf": an["D_out"],
                             "join_for_sigma": J["name"] if J else None, "sigma_semi": sigma,
                             "phi_meas": phi, "phi_model": phi_model, "err_phi_%": pct(phi_model, phi),
                             "D_out_model": phi_model * an["D_out"], "err_D_out_%": pct(phi_model * an["D_out"], ab["D_out"])})
            row["appliers"].append(item)
        out.append(row)
    return out
