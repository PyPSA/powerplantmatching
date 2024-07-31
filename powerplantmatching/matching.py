# Copyright 2016-2018 Fabian Hofmann (FIAS), Jonas Hoersch (KIT, IAI) and
# Fabian Gotzens (FZJ, IEK-STE)

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
Functions for linking and combining different datasets
"""

import logging
from itertools import combinations

import numpy as np
import pandas as pd

from .cleaning import clean_technology
from .core import get_config, get_obj_if_Acc
from .duke import duke
from .utils import get_name, parmap, read_csv_if_string

logger = logging.getLogger(__name__)


def best_matches(links):
    """
    Subsequent to duke() with singlematch=True. Returns reduced list of
    matches on the base of the highest score for each duplicated entry.

    Parameters
    ----------
    links : pd.DataFrame
        Links as returned by duke
    """
    labels = links.columns.difference({"scores"})
    if links.empty:
        return pd.DataFrame(columns=labels)
    else:
        return links.groupby(links.iloc[:, 1], as_index=False, sort=False).apply(
            lambda x: x.loc[x.scores.astype(float).idxmax(), labels]
        )


def compare_two_datasets(dfs, labels, country_wise=True, config=None, **dukeargs):
    """
    Duke-based horizontal match of two databases. Returns the matched
    dataframe including only the matched entries in a multi-indexed
    pandas.Dataframe. Compares all properties of the given columns
    ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different two datasets. The match is in one-to-one
    mode, that is every entry of the initial databases has maximally
    one link in order to obtain unique entries in the resulting
    dataframe.  Attention: When aborting this command, the duke
    process will still continue in the background, wait until the
    process is finished before restarting.

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
    used_deprecated_args = deprecated_args.intersection(dukeargs)
    if used_deprecated_args:
        for arg in used_deprecated_args:
            dukeargs.pop(arg)
        msg = "The following arguments were deprecated and are being ignored: "
        logger.warning(msg + f"{used_deprecated_args}")

    dfs = list(map(read_csv_if_string, dfs))
    if "singlematch" not in dukeargs:
        dukeargs["singlematch"] = True

    def country_link(dfs, country):
        # country_selector for both dataframes
        sel_country_b = [df["Country"] == country for df in dfs]
        # only append if country appears in both dataframse
        if all(sel.any() for sel in sel_country_b):
            return duke(
                [df[sel] for df, sel in zip(dfs, sel_country_b)], labels, **dukeargs
            )
        else:
            return pd.DataFrame(columns=[*labels, "scores"])

    if country_wise:
        countries = config["target_countries"]
        links = [country_link(dfs, c) for c in countries]
        links = [link for link in links if not link.empty]
        if links:
            links = pd.concat(links, ignore_index=True)
        else:
            links = pd.DataFrame(columns=[*labels, "scores"])
    else:
        links = duke(dfs, labels=labels, **dukeargs)

    if links.empty:
        matches = pd.DataFrame(columns=labels)
    else:
        matches = best_matches(links)

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
        scores), obtained from the linkfile (duke() and
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
                    lambda x: x.loc[x.isnull().sum(axis=1).idxmin()]
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
    datasets, labels, use_saved_matches=False, config=None, **dukeargs
):
    """
    Duke-based horizontal match of multiple databases. Returns the
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
        return compare_two_datasets(dfs_lbs[:2], dfs_lbs[2:], config=config, **dukeargs)

    mapargs = [[dfs[c], dfs[d], labels[c], labels[d]] for c, d in combs]
    all_matches = parmap(comp_dfs, mapargs)

    return cross_matches(all_matches, labels=labels)


def combine_multiple_datasets(datasets, labels=None, config=None, **dukeargs):
    """
    Duke-based horizontal match of multiple databases. Returns the
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

    crossmatches = link_multiple_datasets(datasets, labels, config=config, **dukeargs)
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
            "eic_code": set,
        }
    )
    props_for_groups = pd.Series(props_for_groups)[cols].to_dict()

    # set low priority on Fueltype 'Other' and Set 'PP'
    # turn it since aggregating only possible for axis=0
    sdf = (
        df.assign(Set=lambda df: df.Set.where(df.Set != "PP"))
        .assign(Fueltype=lambda df: df.Fueltype.where(df.Set != "Other"))
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
