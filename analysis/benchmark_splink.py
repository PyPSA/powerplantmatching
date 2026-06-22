# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""Benchmark the Splink matching engine against DUKE on a ground truth
recovered from the production ``powerplants.csv`` (projectID clusters).

Run from the repository root with the package installed:

    python analysis/benchmark_splink.py
"""

import ast
import time
import warnings
from itertools import combinations

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

warnings.filterwarnings("ignore")
import logging  # noqa: E402

logging.disable(logging.WARNING)

import powerplantmatching as pm  # noqa: E402
from powerplantmatching import linkage as lk  # noqa: E402
from powerplantmatching.cleaning import cliques  # noqa: E402

try:  # DUKE was removed in this branch; check out master to benchmark against it
    from powerplantmatching.duke import duke
except ImportError:
    duke = None

N_LINK_COUNTRIES = 12
N_DEDUP_COUNTRIES = 8


def ground_truth(path="powerplants.csv"):
    pp = pd.read_csv(path, usecols=["projectID"])
    link, dedup = set(), set()
    for raw in pp["projectID"]:
        d = ast.literal_eval(raw)
        geo, gpd = d.get("GEO", set()), d.get("GPD", set())
        link |= {(g, p) for g in geo for p in gpd}
        dedup |= set(combinations(sorted(gpd), 2))
    return link, dedup


def prf(pred, truth):
    tp = len(pred & truth)
    p = tp / len(pred) if pred else 0.0
    r = tp / len(truth) if truth else 0.0
    f = 2 * p * r / (p + r) if p + r else 0.0
    return p, r, f


def top_countries(gt, pid2country, available, n):
    counts = pd.Series([a for a, _ in gt]).map(pid2country).value_counts()
    return [c for c in counts.index if c in available][:n]


def link_pairs(matcher, geo, gpd, countries, **kw):
    pairs, t0 = set(), time.perf_counter()
    for c in countries:
        gc, pc = geo[geo.Country == c], gpd[gpd.Country == c]
        if gc.empty or pc.empty:
            continue
        links = matcher([gc, pc], labels=["one", "two"], singlematch=True, **kw)
        gp = geo.loc[links["one"], "projectID"].to_numpy()
        pp = gpd.loc[links["two"], "projectID"].to_numpy()
        pairs |= {(a, b) for a, b in zip(gp, pp)}
    return pairs, time.perf_counter() - t0


def _haversine_km(a, b):
    la1, lo1, la2, lo2 = map(np.radians, [a.lat, a.lon, b.lat, b.lon])
    h = np.sin((la2 - la1) / 2) ** 2 + np.cos(la1) * np.cos(la2) * np.sin((lo2 - lo1) / 2) ** 2
    return 2 * 6371 * np.arcsin(np.sqrt(h))


def dedup_pairs(matcher, gpd, countries, **kw):
    pairs, merges, principled, total = set(), 0, 0, 0
    t0 = time.perf_counter()
    for c in countries:
        sub = gpd[gpd.Country == c]
        if len(sub) < 2:
            continue
        groups = cliques(sub, matcher(sub, **kw))
        for _, grp in groups.groupby("grouped"):
            ids = sorted(grp.projectID.unique())
            if len(ids) < 2:
                continue
            merges += 1
            pairs |= set(combinations(ids, 2))
            for a, b in combinations(grp.itertuples(), 2):
                total += 1
                name_sim = fuzz.token_set_ratio(str(a.Name), str(b.Name)) / 100
                far = pd.isna(a.lat) or pd.isna(b.lat) or _haversine_km(a, b) > 5
                principled += name_sim >= 0.6 and not far and a.Fueltype == b.Fueltype
    return pairs, time.perf_counter() - t0, merges, principled / total if total else 0.0


def main():
    gt_link, gt_dedup = ground_truth()
    geo, gpd = pm.data.GEO(), pm.data.GPD()
    geo_ids, gpd_ids = set(geo.projectID), set(gpd.projectID)
    gt_link = {(g, p) for g, p in gt_link if g in geo_ids and p in gpd_ids}
    gt_dedup = {(a, b) for a, b in gt_dedup if a in gpd_ids and b in gpd_ids}

    shared = set(geo.Country) & set(gpd.Country)
    geo_c = geo.set_index("projectID").Country.to_dict()
    gpd_c = gpd.set_index("projectID").Country.to_dict()
    link_countries = top_countries(gt_link, geo_c, shared, N_LINK_COUNTRIES)
    dedup_countries = top_countries(gt_dedup, gpd_c, set(gpd.Country), N_DEDUP_COUNTRIES)

    print(f"== record linkage (GEO x GPD, {len(link_countries)} countries) ==")
    print(f"ground-truth pairs: {len(gt_link)}")
    if duke is not None:
        dp, dt = link_pairs(duke, geo, gpd, link_countries)
        p, r, f = prf(dp, gt_link)
        print(f"DUKE        P={p:.3f} R={r:.3f} F1={f:.3f}  ({dt:.1f}s)")
    for th in [0.5, 0.7, 0.9, 0.95]:
        sp, st = link_pairs(lk.match, geo, gpd, link_countries, threshold=th)
        p, r, f = prf(sp, gt_link)
        print(f"splink {th:.2f}  P={p:.3f} R={r:.3f} F1={f:.3f}  ({st:.1f}s)")

    print(f"\n== deduplication (GPD, {len(dedup_countries)} countries) ==")
    print(f"ground-truth pairs (circular, DUKE-derived): {len(gt_dedup)}")
    if duke is not None:
        dp, dt, dm, _ = dedup_pairs(duke, gpd, dedup_countries)
        p, r, f = prf(dp, gt_dedup)
        print(f"DUKE        merges={dm} P={p:.3f} R={r:.3f} F1={f:.3f}  ({dt:.1f}s)")
    for th in [0.9, 0.95, 0.99]:
        sp, st, sm, frac = dedup_pairs(lk.match, gpd, dedup_countries, threshold=th)
        p, r, f = prf(sp, gt_dedup)
        print(
            f"splink {th:.2f}  merges={sm} P={p:.3f} R={r:.3f} "
            f"F1={f:.3f} principled={100 * frac:.0f}%  ({st:.1f}s)"
        )


if __name__ == "__main__":
    main()
