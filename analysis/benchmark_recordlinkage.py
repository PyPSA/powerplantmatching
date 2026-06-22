# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Benchmark the recordlinkage prototype against DUKE on real GEO x GPD slices,
scored against an objective ground truth: the GEO<->GPD pairs that the
production pipeline placed in the same cluster (powerplants.csv projectID).

Reports precision / recall / F1 for each backend, wall-clock time, and a
recordlinkage threshold calibration.
"""

import ast
import time

import numpy as np
import pandas as pd

import powerplantmatching as pm
from powerplantmatching import duke as duke_mod
from powerplantmatching import duke_recordlinkage as rl_mod
from powerplantmatching.matching import best_matches

COUNTRIES = [
    "France", "Spain", "United Kingdom", "Austria", "Italy", "Sweden",
    "Switzerland", "Greece", "Netherlands", "Germany", "Poland", "Ukraine",
]


def build_ground_truth(geo, gpd):
    """GEO<->GPD index pairs that share a production cluster in powerplants.csv."""
    pp = pd.read_csv("powerplants.csv")
    geo_id2idx = {v: i for i, v in geo["projectID"].dropna().items()}
    gpd_id2idx = {v: i for i, v in gpd["projectID"].dropna().items()}

    def ids(d, k):
        try:
            v = ast.literal_eval(d).get(k)
            return set(v) if v else set()
        except (ValueError, SyntaxError):
            return set()

    pairs = set()
    for d in pp["projectID"].dropna():
        for g in ids(d, "GEO"):
            for p in ids(d, "GPD"):
                if g in geo_id2idx and p in gpd_id2idx:
                    pairs.add((geo_id2idx[g], gpd_id2idx[p]))
    return pairs


def predict(backend, left, right, **kw):
    t0 = time.perf_counter()
    links = backend([left, right], labels=["one", "two"], singlematch=True, **kw)
    matches = best_matches(links) if not links.empty else links
    dt = time.perf_counter() - t0
    pairs = set(zip(matches["one"].astype(int), matches["two"].astype(int)))
    return pairs, dt


def score(pred, truth):
    tp = len(pred & truth)
    precision = tp / len(pred) if pred else np.nan
    recall = tp / len(truth) if truth else np.nan
    f1 = 2 * precision * recall / (precision + recall) if tp else 0.0
    return precision, recall, f1


def dedup_report(geo, gpd):
    """Show that recordlinkage dedup merges are principled, not over-eager."""
    from rapidfuzz import fuzz

    from powerplantmatching import duke_recordlinkage as rl

    sample = geo.sort_values("Name").head(200).reset_index(drop=True)
    merged = rl.duke(sample)
    print("\n=== dedup: GEO.head(200) -> recordlinkage collapses unit-duplicates ===")
    print(f"units in: {len(sample)}   distinct dup-pairs: {len(merged) // 2}")

    n = principled = 0
    for c in gpd.Country.unique():
        sub = gpd[gpd.Country == c].reset_index(drop=True)
        if len(sub) < 2:
            continue
        out = rl.duke(sub)
        for key in {frozenset((int(a), int(b))) for a, b in zip(out.one, out.two)}:
            i, j = tuple(key)
            ra, rb = sub.loc[i], sub.loc[j]
            n += 1
            nsim = fuzz.token_set_ratio(str(ra.Name).lower(), str(rb.Name).lower())
            gs = rl._geo_matrix(sub.loc[[i]], sub.loc[[j]])[0][0, 0]
            if nsim >= 85 and gs > 0 and ra.Fueltype == rb.Fueltype:
                principled += 1
    print(f"GPD dedup merges: {n}   principled "
          f"(name>=85 & within 5km & same fueltype): {principled} ({100 * principled / n:.0f}%)")


def main():
    geo, gpd = pm.data.GEO(), pm.data.GPD()
    gt = build_ground_truth(geo, gpd)

    backends = {"DUKE": duke_mod.duke, "recordlinkage": rl_mod.duke}
    pred = {k: set() for k in backends}
    times = {k: 0.0 for k in backends}
    gt_slice = set()

    for c in COUNTRIES:
        left, right = geo[geo.Country == c], gpd[gpd.Country == c]
        if left.empty or right.empty:
            continue
        lidx, ridx = set(left.index), set(right.index)
        gt_slice |= {(g, p) for (g, p) in gt if g in lidx and p in ridx}
        for name, backend in backends.items():
            p, dt = predict(backend, left, right)
            pred[name] |= p
            times[name] += dt

    print(f"\n=== GEO x GPD vs production ground truth ({len(gt_slice)} pairs) ===\n")
    rows = []
    for name in backends:
        pr, rc, f1 = score(pred[name], gt_slice)
        rows.append({
            "backend": name, "matches": len(pred[name]),
            "precision": round(pr, 3), "recall": round(rc, 3), "f1": round(f1, 3),
            "time_s": round(times[name], 1),
        })
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n=== recordlinkage threshold calibration (vs ground truth) ===\n")
    raw = {}
    for c in COUNTRIES:
        left, right = geo[geo.Country == c], gpd[gpd.Country == c]
        if left.empty or right.empty:
            continue
        raw[c] = (rl_mod.duke([left, right], labels=["one", "two"], singlematch=True,
                              threshold=0.0), left, right)
    sweep = []
    for th in np.round(np.arange(0.95, 0.9991, 0.005), 4):
        pr_pairs = set()
        for links, _, _ in raw.values():
            kept = best_matches(links[links.scores >= th])
            if not kept.empty:
                pr_pairs |= set(zip(kept["one"].astype(int), kept["two"].astype(int)))
        pr, rc, f1 = score(pr_pairs, gt_slice)
        sweep.append({"threshold": th, "matches": len(pr_pairs),
                      "precision": round(pr, 3), "recall": round(rc, 3), "f1": round(f1, 3)})
    sweep = pd.DataFrame(sweep)
    print(sweep.to_string(index=False))
    best = sweep.loc[sweep["f1"].idxmax()]
    print(f"\nBest RL F1={best.f1} at threshold={best.threshold} "
          f"(P={best.precision}, R={best.recall})")

    dedup_report(geo, gpd)


if __name__ == "__main__":
    main()
