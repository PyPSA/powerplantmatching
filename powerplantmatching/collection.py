# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Processed datasets of merged and/or adjusted data
"""

import hashlib
import logging
import os
import shutil
import warnings

import pandas as pd
from deprecation import deprecated

from .cleaning import aggregate_units
from .core import _data_in, _data_out, get_config
from .heuristics import extend_by_non_matched, extend_by_VRE
from .matching import combine_multiple_datasets, reduce_matched_dataframe
from .utils import (
    parmap,
    parse_string_to_dict,
    set_column_name,
    to_dict_if_string,
)

logger = logging.getLogger(__name__)


def collect(
    datasets,
    update=False,
    reduced=True,
    config=None,
    **dukeargs,
):
    """
    Return the collection for a given list of datasets in matched or
    reduced form.

    Parameters
    ----------
    datasets : list or str
        list containing the dataset identifiers as str, or single str
    update : bool
        Do an horizontal update (True) or read from the cache file (False)
    reduced : bool
        Switch as to return the reduced (True) or matched (False) dataset.
    config : dict
        Configuration file of powerplantmatching
    **dukeargs : keyword-args for duke
    """

    from . import data

    if config is None:
        config = get_config()

    def df_by_name(name):
        conf = config[name]
        get_df = getattr(data, name)
        df = get_df(config=config)

        for source in config["matching_sources"]:
            if isinstance(source, dict) and next(iter(source)) == name:
                df = df.query(source[name])

        if not conf.get("aggregated_units", False):
            return aggregate_units(df, dataset_name=name, config=config)
        else:
            return df.assign(projectID=df.projectID.map(lambda x: {x}))

    # Deal with the case that only one dataset is requested
    if isinstance(datasets, str):
        return df_by_name(datasets)

    datasets = sorted(datasets)
    logger.info("Create combined dataset for {}".format(", ".join(datasets)))

    fn = "_".join(map(str.upper, datasets))
    outfn_matched = _data_out(f"Matched_{fn}.csv", config)

    fn = "_".join(map(str.upper, datasets))
    outfn_reduced = _data_out(f"Matched_{fn}_reduced.csv", config)

    if not update and not os.path.exists(outfn_reduced if reduced else outfn_matched):
        logger.warning("Forcing update since the cache file is missing")
        update = True

    if update:
        dfs = parmap(df_by_name, datasets)
        matched = combine_multiple_datasets(dfs, datasets, config=config, **dukeargs)
        (
            matched.assign(projectID=lambda df: df.projectID.astype(str)).to_csv(
                outfn_matched, index_label="id"
            )
        )

        reduced_df = reduce_matched_dataframe(matched, config=config)
        reduced_df.to_csv(outfn_reduced, index_label="id")

        return reduced_df if reduced else matched
    else:
        if reduced:
            df = pd.read_csv(outfn_reduced, index_col=0)
        else:
            df = pd.read_csv(
                outfn_matched, index_col=0, header=[0, 1], low_memory=False
            )
        return df.pipe(parse_string_to_dict, ["projectID", "EIC"])


def _read_matched(
    fn: str, header: int | list[int], config: dict, extend_by_vres: bool
) -> pd.DataFrame:
    """Read a stored matched dataset, optionally extended by VRE."""
    df = (
        pd.read_csv(fn, index_col=0, header=header)
        .pipe(parse_string_to_dict, ["projectID", "EIC"])
        .pipe(set_column_name, "Matched Data")
    )
    if extend_by_vres:
        df = extend_by_VRE(df, config=config, base_year=config["opsd_vres_base_year"])
    return df


def powerplants(
    config=None,
    config_update=None,
    update=False,
    from_url: bool | str = False,
    extend_by_vres=False,
    extendby_kwargs=None,
    extend_by_kwargs=None,
    fill_geopositions=True,
    filter_missing_geopositions=True,
    **collection_kwargs,
):
    """
    Return the full matched dataset including all data sources listed in
    config.yaml/matching_sources. The combined data is additionally extended
    by non-matched entries of sources given in
    config.yaml/fully_included_sources.


    Parameters
    ----------
    update : Boolean, default False
            Whether to rerun the matching process. Overrides stored to False
            if True.
    from_url: bool or str, default False
            Load a released dataset from an online resource.
            `True` loads the release matching the currently installed version of 
            powerplantmatching and stores it as 'build' version, so that a
            later call with `from_url=False` returns it.
            Other released versions can be loaded by specifying
            a string starting with "v" as release tag, e.g. "v0.9.0".
            An arbitrary URL starting with "http(s)://" can also be specified but
            needs to match the expected format, else unforseen behaviour is to be
            expected.
            All downloads are cached under `<data_dir>/data/in/releases`.
            Use `update=True` to refresh the cache and download the file again.
    config : Dict, default None
            Define a configuration varying from the setting in config.yaml.
            Relevant keywords are 'matching_sources', 'fully_included_sources'.
    config_update : Dict, default None
            Configuration input dictionary to be merged into the default
            configuration data
    extend_by_vres : Boolean, default False
            Deprecated since v0.10.0 and will be removed in v0.11.0. Wind and
            solar power plants are already included from GEM and MASTR. This
            option adds them again from the outdated
            powerplantmatching.data.OPSD_VRE() and counts them twice.
    extendby_kwargs : Dict,
            Dict of keyword arguments passed to powerplantmatchting.
            heuristics.extend_by_non_matched
    fill_geopositions: Boolean, default True
            Whether to fill geo coordinates by calling
            `df.powerplant.fill_geoposition()` after the matching process
            and before the optional extension by VRES. Only active if
            `update` is true.
    filter_missing_geopositions: Boolean, default True
            Whether to filter out resulting entries without geo coordinates. The
            filtering happens after the matching process and the optional filling of
            geo coordinates and before the optional extension by VRES. Only active
            if `update` is true.
    **collection_kwargs : kwargs
            Arguments passed to powerplantmatching.collection.Collection.

    """
    from . import latest_release

    if extend_by_vres:
        warnings.warn(
            "`extend_by_vres` is deprecated and will be removed in v0.11.0. Wind "
            "and solar power plants are already included from GEM and MASTR. "
            "This option adds them again from outdated OPSD data (2020).",
            FutureWarning,
            stacklevel=2,
        )

    if config is None:
        if config_update is None:
            config = get_config()
        else:
            config = get_config(**config_update)
    if extendby_kwargs is None:
        extendby_kwargs = {}
    if extend_by_kwargs is None:
        extend_by_kwargs = {}

    deprecated_args = {"update_all", "stored"}
    used_deprecated_args = deprecated_args.intersection(collection_kwargs.keys())
    if used_deprecated_args:
        msg = "The following arguments were deprecated and are being ignored: "
        logger.warning(msg + f"{used_deprecated_args}")
    if extendby_kwargs:
        warnings.warn(
            "`extendby_kwargs` is deprecated in favor of `extend_by_kwargs`",
            FutureWarning,
            stacklevel=2,
        )
        extend_by_kwargs.update(extendby_kwargs)

    collection_kwargs.setdefault("update", update)

    if from_url:
        latest = from_url is True
        if latest:
            # Take latest release known to the package
            from_url = "v" + latest_release
        if not isinstance(from_url, str):
            raise TypeError(f"`from_url` must be bool or str, got {type(from_url)}.")
        if from_url.startswith("v"):
            # Take a specific released dataset
            url = config["matched_data_url"].format(tag=from_url)
        else:
            # Try to read from arbitrary URL (unsafe?)
            url = from_url
        if not url.startswith(("http://", "https://")):
            raise ValueError(
                f"'{url}' is not an http(s) URL. `from_url` takes a release tag "
                "starting with 'v' or an http(s) URL, and `matched_data_url` in "
                "the config must be an http(s) URL."
            )
        md5 = hashlib.md5(url.encode(), usedforsecurity=False).hexdigest()
        fn = _data_in(os.path.join("releases", md5 + ".csv"))
        if update or not os.path.exists(fn):
            logger.info(f"Retrieving data from {url}")
            os.makedirs(os.path.dirname(fn), exist_ok=True)
            # read fully before writing, so a failed download leaves no cache file
            pd.read_csv(url, index_col=0).to_csv(fn)
            logger.info(f"Store data at {fn}")
        if latest:
            # if the installed version's data is retrieved, it doubles as local build output;
            # subsequent calls to `powerplants()` returns it even without the `from_url` argument.
            # I think this is a bug because a different local configuration != default config
            # is overwritten through this without warning.
            # But it has been standard behaviour for so long I'd take it as a feature at this point
            shutil.copyfile(fn, _data_out("matched_data_red.csv", config))
        return _read_matched(fn, 0, config, extend_by_vres)

    if collection_kwargs.get("reduced", True):
        fn = _data_out("matched_data_red.csv", config)
        header: int | list[int] = 0
    else:
        fn = _data_out("matched_data.csv", config)
        header = [0, 1]

    if not update and os.path.exists(fn):
        return _read_matched(fn, header, config, extend_by_vres)

    matching_sources = [
        list(to_dict_if_string(a))[0] for a in config["matching_sources"]
    ]
    matched = collect(matching_sources, config=config, **collection_kwargs)

    if isinstance(config["fully_included_sources"], list):
        for source in config["fully_included_sources"]:
            source = to_dict_if_string(source)
            (name,) = list(source)
            extend_by_kwargs.update({"query": source[name]})
            matched = extend_by_non_matched(
                matched, name, config=config, **extend_by_kwargs
            )

    if fill_geopositions:
        matched = matched.powerplant.fill_geoposition()

    if filter_missing_geopositions:
        if isinstance(matched.columns, pd.MultiIndex):
            matched = matched[matched.lat.notnull().any(axis=1)]
        else:
            matched = matched[matched.lat.notnull()]

    matched.reset_index(drop=True).to_csv(fn, index_label="id", encoding="utf-8")

    if extend_by_vres:
        matched = extend_by_VRE(
            matched, config=config, base_year=config["opsd_vres_base_year"]
        )
    return matched.pipe(set_column_name, "Matched Data")


@deprecated(deprecated_in="5.5", details="Use `powerplants` instead.")
def matched_data(
    config=None,
    update=False,
    from_url=False,
    extend_by_vres=False,
    extendby_kwargs={},
    extend_by_kwargs={},
    fill_geopositions=True,
    filter_missing_geopositions=True,
    **collection_kwargs,
):
    return powerplants(
        config=config,
        update=update,
        from_url=from_url,
        extend_by_vres=extend_by_vres,
        extendby_kwargs=extendby_kwargs,
        extend_by_kwargs=extend_by_kwargs,
        fill_geopositions=fill_geopositions,
        filter_missing_geopositions=filter_missing_geopositions,
        **collection_kwargs,
    )
