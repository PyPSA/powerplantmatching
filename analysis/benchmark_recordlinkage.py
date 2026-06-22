# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Benchmark the recordlinkage prototype against the DUKE backend on real
GEO vs GPD country slices: agreement on matched pairs, score correlation,
and wall-clock timing.
"""

import time

import numpy as np
import pandas as pd

import powerplantmatching as pm
from powerplantmatching import duke as duke_mod
from powerplantmatching import duke_recordlinkage as rl_mod
from powerplantmatching.matching import best_matches

COUNTRIES = ["Germany", "France", "Spain", "Italy", "Poland", "Sweden"]


def run_backend(backend, left, right):
    t0 = time.perf_counter()
    links = backend([left, right], labels=["one", "two"], singlematch=True)
    matches = best_matches(links) if not links.empty else links
    dt = time.perf_counter() - t0
    pairs = set(zip(matches["one"].astype(int), matches["two"].astype(int)))
    return pairs, dt, links


def evaluate(geo, gpd, country):
    left = geo[geo.Country == country]
    right = gpd[gpd.Country == country]
    if left.empty or right.empty:
        return None

    duke_pairs, duke_dt, _ = run_backend(duke_mod.duke, left, right)
    rl_pairs, rl_dt, _ = run_backend(rl_mod.duke, left, right)

    inter = duke_pairs & rl_pairs
    union = duke_pairs | rl_pairs
    jaccard = len(inter) / len(union) if union else 1.0
    recall = len(inter) / len(duke_pairs) if duke_pairs else np.nan
    precision = len(inter) / len(rl_pairs) if rl_pairs else np.nan

    return {
        "country": country,
        "n_left": len(left),
        "n_right": len(right),
        "duke_matches": len(duke_pairs),
        "rl_matches": len(rl_pairs),
        "agree": len(inter),
        "jaccard": round(jaccard, 3),
        "recall_vs_duke": round(recall, 3) if duke_pairs else np.nan,
        "precision_vs_duke": round(precision, 3) if rl_pairs else np.nan,
        "duke_s": round(duke_dt, 2),
        "rl_s": round(rl_dt, 2),
    }


def calibrate(geo, gpd, thresholds):
    """Sweep the recordlinkage threshold to find the best F1 agreement with DUKE."""
    duke_by_country = {}
    rl_links_by_country = {}
    for c in COUNTRIES:
        left, right = geo[geo.Country == c], gpd[gpd.Country == c]
        if left.empty or right.empty:
            continue
        duke_by_country[c], _, _ = run_backend(duke_mod.duke, left, right)
        links = rl_mod.duke([left, right], labels=["one", "two"], singlematch=True)
        rl_links_by_country[c] = (links, left, right)

    rows = []
    for th in thresholds:
        agree = duke_n = rl_n = 0
        for c, (links, _, _) in rl_links_by_country.items():
            kept = links[links.scores >= th]
            rl_pairs = set(zip(kept["one"].astype(int), kept["two"].astype(int)))
            duke_pairs = duke_by_country[c]
            agree += len(duke_pairs & rl_pairs)
            duke_n += len(duke_pairs)
            rl_n += len(rl_pairs)
        recall = agree / duke_n if duke_n else 0
        precision = agree / rl_n if rl_n else 0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0
        rows.append(
            {"threshold": th, "rl_matches": rl_n, "recall": round(recall, 3),
             "precision": round(precision, 3), "f1": round(f1, 3)}
        )
    return pd.DataFrame(rows)


def main():
    geo = pm.data.GEO()
    gpd = pm.data.GPD()

    rows = [r for c in COUNTRIES if (r := evaluate(geo, gpd, c)) is not None]
    table = pd.DataFrame(rows)
    pd.set_option("display.width", 200, "display.max_columns", 20)
    print("\n=== DUKE vs recordlinkage (GEO x GPD, singlematch best_matches) ===\n")
    print(table.to_string(index=False))

    tot_duke = table["duke_matches"].sum()
    tot_rl = table["rl_matches"].sum()
    tot_agree = table["agree"].sum()
    print(
        f"\nTotals: DUKE={tot_duke} matches, RL={tot_rl} matches, "
        f"agree={tot_agree} "
        f"(recall={tot_agree / tot_duke:.3f}, precision={tot_agree / tot_rl:.3f})"
    )
    print(
        f"Time: DUKE={table['duke_s'].sum():.1f}s, RL={table['rl_s'].sum():.1f}s "
        f"(speedup x{table['duke_s'].sum() / max(table['rl_s'].sum(), 1e-6):.1f})"
    )

    print("\n=== recordlinkage threshold calibration vs DUKE ===\n")
    sweep = calibrate(geo, gpd, np.round(np.arange(0.90, 0.991, 0.01), 3))
    print(sweep.to_string(index=False))
    best = sweep.loc[sweep["f1"].idxmax()]
    print(f"\nBest F1={best.f1} at threshold={best.threshold} "
          f"(recall={best.recall}, precision={best.precision})")


if __name__ == "__main__":
    main()
