# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Functions for linking and combining different datasets
"""

import logging
from collections.abc import Hashable, Sequence
from itertools import combinations

import numpy as np
import pandas as pd

from .cleaning import clean_technology
from .core import get_config, get_obj_if_Acc
from .linkage import match, select_one_to_one
from .utils import collect_eic_codes, get_name, parmap, read_csv_if_string

logger = logging.getLogger(__name__)


def _match_by_eic(
    df0: pd.DataFrame, df1: pd.DataFrame, labels: Sequence[str]
) -> tuple[pd.DataFrame, set[Hashable], set[Hashable]]:
    """
    Deterministic matching of two datasets by EIC (Energy Identification Code).

    Accept only isolated one-to-one links in the shared-code graph. Scheme
    identifiers shared by several stations remain available for fuzzy matching.

    Parameters
    ----------
    df0, df1 : pd.DataFrame
        Source dataframes with an 'EIC' column containing scalar or collected codes
        (as produced by ``aggregate_units``).
    labels : list of str
        Two-element list of dataset names for the output columns.

    Returns
    -------
    matches : pd.DataFrame
        DataFrame with columns ``labels``, containing matched index pairs.
    matched_idx0 : set
        Indices from df0 that were matched.
    matched_idx1 : set
        Indices from df1 that were matched.
    """
    empty = pd.DataFrame(columns=labels), set(), set()

    if len(labels) != 2 or labels[0] == labels[1] or "EIC" in labels:
        raise ValueError(
            "EIC matching requires two distinct source labels other than EIC"
        )
    if not df0.index.is_unique or not df1.index.is_unique:
        raise ValueError("EIC matching requires a unique index in each source")
    if "EIC" not in df0.columns or "EIC" not in df1.columns:
        return empty

    def codes(df: pd.DataFrame, label: str) -> pd.DataFrame:
        expanded = df["EIC"].explode().dropna()
        expanded = expanded[expanded.map(lambda value: isinstance(value, str))]
        return expanded[expanded.ne("")].rename_axis(label).reset_index(name="EIC")

    links = pd.merge(codes(df0, labels[0]), codes(df1, labels[1]), on="EIC")
    links = links[list(labels)].drop_duplicates()
    if links.empty:
        return empty

    isolated = links.groupby(labels[0])[labels[1]].transform("size").eq(
        1
    ) & links.groupby(labels[1])[labels[0]].transform("size").eq(1)
    matches = links.loc[isolated].reset_index(drop=True)
    matched_idx0 = set(matches[labels[0]])
    matched_idx1 = set(matches[labels[1]])

    logger.info(
        "EIC matching: %d deterministic matches between `%s` and `%s`",
        len(matches), labels[0], labels[1],
    )

    return matches, matched_idx0, matched_idx1


def best_matches(links: pd.DataFrame) -> pd.DataFrame:
    """Select a maximum-score one-to-one assignment of accepted links."""
    return select_one_to_one(links).drop(columns="scores")


def compare_two_datasets(dfs, labels, country_wise=True, config=None, **matchargs):
    """
    Fuzzy horizontal match of two databases. Returns the matched
    dataframe including only the matched entries in a multi-indexed
    pandas.Dataframe. Compares all properties of the given columns
    ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different two datasets. The match is in one-to-one
    mode, that is every entry of the initial databases has maximally
    one link in order to obtain unique entries in the resulting
    dataframe.

    Parameters
    ----------
    dfs : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases for the resulting dataframe


    """
    if config is None:
        config = get_config()

    deprecated_args = {"use_saved_matches", "use_saved_aggregation"}
    used_deprecated_args = deprecated_args.intersection(matchargs)
    if used_deprecated_args:
        for arg in used_deprecated_args:
            matchargs.pop(arg)
        msg = "The following arguments were deprecated and are being ignored: "
        logger.warning(msg + f"{used_deprecated_args}")

    dfs = list(map(read_csv_if_string, dfs))
    if "singlematch" not in matchargs:
        matchargs["singlematch"] = True

    # Resolve isolated exact-EIC pairs before fuzzy matching.
    eic_matches, matched_idx0, matched_idx1 = _match_by_eic(dfs[0], dfs[1], labels)

    # Remove EIC-matched rows from the fuzzy input
    remaining = [
        dfs[0].drop(index=matched_idx0, errors="ignore"),
        dfs[1].drop(index=matched_idx1, errors="ignore"),
    ]

    # Compare only the residual records.
    def country_link(dfs, country):
        # country_selector for both dataframes
        sel_country_b = [df["Country"] == country for df in dfs]
        # only append if country appears in both dataframes
        if all(sel.any() for sel in sel_country_b):
            return match(
                [df[sel] for df, sel in zip(dfs, sel_country_b)], labels, **matchargs
            )
        else:
            return pd.DataFrame(columns=[*labels, "scores"])

    if country_wise:
        countries = config["target_countries"]
        links = [country_link(remaining, c) for c in countries]
        links = [link for link in links if not link.empty]
        if links:
            links = pd.concat(links, ignore_index=True)
        else:
            links = pd.DataFrame(columns=[*labels, "scores"])
    else:
        links = match(remaining, labels=labels, **matchargs)

    if links.empty:
        fuzzy_matches = pd.DataFrame(columns=labels)
    else:
        fuzzy_matches = best_matches(links)

    # Combine disjoint exact and fuzzy pairs.
    matches = pd.concat([eic_matches, fuzzy_matches], ignore_index=True)
    return matches


def cross_matches(sets_of_pairs, labels=None):
    """
    Combines multiple sets of pairs and returns one consistent
    dataframe. Identifiers of two datasets can appear in one row even
    though they did not match directly but indirectly through a
    connecting identifier of another database.

    Parameters
    ----------
    sets_of_pairs : list
        list of pd.Dataframe's containing only the matches (without
        scores), obtained from the linkfile (match() and
        best_matches())
    labels : list of strings
        list of names of the databases, used for specifying the order
        of the output

    """
    m_all = sets_of_pairs
    if labels is None:
        labels = np.unique([x.columns for x in m_all])
    matches = None
    for label in labels:
        base = [m.set_index(label) for m in m_all if label in m and not m.empty]
        if base:
            match_base = pd.concat(base, axis=1).reset_index()
            if matches is None:
                matches = match_base.reindex(columns=labels)
            else:
                matches = pd.concat([matches, match_base], sort=True)

    if matches is None or matches.empty:
        logger.warning("No matches found")
        return pd.DataFrame(columns=labels)

    if matches.isnull().all().any():
        cols = ", ".join(matches.columns[matches.isnull().all()])
        logger.warning(f"No matches found for data source {cols}")

    matches = matches.drop_duplicates().reset_index(drop=True)
    for label in labels:
        matches = pd.concat(
            [
                matches.groupby(label, as_index=False, sort=False).apply(
                    lambda x: x.loc[x.isnull().sum(axis=1).idxmin()],
                    include_groups=False,
                ),
                matches[matches[label].isnull()],
            ]
        ).reset_index(drop=True)
    return (
        matches.assign(length=matches.notna().sum(axis=1))
        .sort_values(by="length", ascending=False)
        .reset_index(drop=True)
        .drop("length", axis=1)
        .reindex(columns=labels)
    )


def link_multiple_datasets(
    datasets, labels, use_saved_matches=False, config=None, **matchargs
):
    """
    Fuzzy horizontal match of multiple databases. Returns the
    matching indices of the datasets. Compares all properties of the
    given columns ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different datasets. The match is in one-to-one mode,
    that is every entry of the initial databases has maximally one
    link to the other database.  This leads to unique entries in the
    resulting dataframe.

    Parameters
    ----------
    datasets : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases in alphabetical order and corresponding
        order to the datasets
    """
    if config is None:
        config = get_config()

    dfs = list(map(read_csv_if_string, datasets))
    labels = [get_name(df) for df in dfs]

    combs = list(combinations(range(len(labels)), 2))

    def comp_dfs(dfs_lbs):
        logger.info("Comparing data sources `{}` and `{}`".format(*dfs_lbs[2:]))
        return compare_two_datasets(
            dfs_lbs[:2], dfs_lbs[2:], config=config, **matchargs
        )

    mapargs = [[dfs[c], dfs[d], labels[c], labels[d]] for c, d in combs]
    all_matches = parmap(comp_dfs, mapargs)

    return cross_matches(all_matches, labels=labels)


def combine_multiple_datasets(datasets, labels=None, config=None, **matchargs):
    """
    Fuzzy horizontal match of multiple databases. Returns the
    matched dataframe including only the matched entries in a
    multi-indexed pandas.Dataframe. Compares all properties of the
    given columns ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different datasets. The match is in one-to-one mode,
    that is every entry of the initial databases has maximally one
    link to the other database.  This leads to unique entries in the
    resulting dataframe.

    Parameters
    ----------
    datasets : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases in alphabetical order and corresponding
        order to the datasets
    """
    if config is None:
        config = get_config()

    def combined_dataframe(cross_matches, datasets, config):
        """
        Use this function to create a matched dataframe on base of the
        cross matches and a list of the databases. Always order the
        database alphabetically.

        Parameters
        ----------
        cross_matches : pandas.Dataframe of the matching indexes of
            the databases, created with
            powerplant_collection.cross_matches()
        datasets : list of pandas.Dataframes or csv-files in the same
            order as in cross_matches
        """
        datasets = list(map(read_csv_if_string, datasets))
        for i, data in enumerate(datasets):
            datasets[i] = data.reindex(cross_matches.iloc[:, i]).reset_index(drop=True)
        return (
            pd.concat(datasets, axis=1, keys=cross_matches.columns.tolist())
            .reorder_levels([1, 0], axis=1)
            .reindex(columns=config["target_columns"], level=0)
            .reset_index(drop=True)
        )

    crossmatches = link_multiple_datasets(datasets, labels, config=config, **matchargs)
    return combined_dataframe(crossmatches, datasets, config).reindex(
        columns=config["target_columns"], level=0
    )


def reduce_matched_dataframe(df, show_orig_names=False, config=None):
    """
    Reduce a matched dataframe to a unique set of columns. For each entry
    take the value of the most reliable data source included in that match.

    Parameters
    ----------
    df : pandas.Dataframe
        MultiIndex dataframe with the matched powerplants, as obtained from
        combined_dataframe() or match_multiple_datasets()
    """
    df = get_obj_if_Acc(df)

    if config is None:
        config = get_config()

    # define which databases are present and get their reliability_score
    sources = df.columns.levels[1]
    rel_scores = pd.Series(
        {s: config[s]["reliability_score"] for s in sources}, dtype=float
    ).sort_values(ascending=False)
    cols = config["target_columns"]
    props_for_groups = {col: "first" for col in cols}
    props_for_groups.update(
        {
            "DateIn": "min",
            "DateRetrofit": "max",
            "DateOut": "max",
            "projectID": lambda x: dict(x.droplevel(0).dropna()),
            "EIC": collect_eic_codes,
        }
    )
    props_for_groups = pd.Series(props_for_groups)[cols].to_dict()

    # set low priority on Fueltype 'Other' and Set 'PP'
    # turn it since aggregating only possible for axis=0
    sdf = (
        df.assign(Set=lambda df: df.Set.where(df.Set != "PP"))
        .assign(Fueltype=lambda df: df.Fueltype.where(df.Fueltype != "Other"))
        .stack(1, future_stack=True)
        .reindex(rel_scores.index, level=1)
        .groupby(level=0)
        .agg(props_for_groups)
        .assign(Set=lambda df: df.Set.fillna("PP"))
        .assign(Fueltype=lambda df: df.Fueltype.fillna("Other"))
    )

    if show_orig_names:
        sdf = sdf.assign(**dict(df.Name))
    return sdf.pipe(clean_technology).reset_index(drop=True)
