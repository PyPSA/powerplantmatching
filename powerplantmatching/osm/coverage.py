"""
Cache coverage utilities for OSM module.

This module provides functions to analyze OSM cache coverage and statistics,
including comparison with live Overpass API counts to detect outdated entries.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import pycountry
from tqdm import tqdm

from powerplantmatching.core import _data_in, get_config

from .cache import ElementCache
from .client import OverpassAPIClient

logger = logging.getLogger(__name__)


def format_table(data, headers, col_widths=None):
    """Simple table formatter to replace tabulate."""
    if not data:
        return ""

    # Calculate column widths if not provided
    if col_widths is None:
        col_widths = [len(str(h)) for h in headers]
        for row in data:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell)))

    # Create format string
    format_str = " | ".join(f"{{:{w}s}}" for w in col_widths)

    # Build table
    lines = []

    # Header
    lines.append(format_str.format(*[str(h) for h in headers]))
    lines.append("-" * (sum(col_widths) + 3 * (len(headers) - 1)))

    # Data rows
    for row in data:
        lines.append(format_str.format(*[str(cell) for cell in row]))

    return "\n".join(lines)


def get_continent_mapping() -> dict[str, str]:
    """
    Get mapping of country codes to continents.

    Returns
    -------
    dict[str, str]
        dictionary mapping ISO country codes to continent names
    """
    return {
        "AW": "North America",
        "AF": "Asia",
        "AO": "Africa",
        "AI": "North America",
        "AX": "Europe",
        "AL": "Europe",
        "AD": "Europe",
        "AE": "Asia",
        "AR": "South America",
        "AM": "Asia",
        "AS": "Oceania",
        "AG": "North America",
        "AU": "Oceania",
        "AT": "Europe",
        "AZ": "Asia",
        "BI": "Africa",
        "BE": "Europe",
        "BJ": "Africa",
        "BQ": "North America",
        "BF": "Africa",
        "BD": "Asia",
        "BG": "Europe",
        "BH": "Asia",
        "BS": "North America",
        "BA": "Europe",
        "BL": "North America",
        "BY": "Europe",
        "BZ": "North America",
        "BM": "North America",
        "BO": "South America",
        "BR": "South America",
        "BB": "North America",
        "BN": "Asia",
        "BT": "Asia",
        "BV": "Antarctica",
        "BW": "Africa",
        "CF": "Africa",
        "CA": "North America",
        "CC": "Asia",
        "CH": "Europe",
        "CL": "South America",
        "CN": "Asia",
        "CI": "Africa",
        "CM": "Africa",
        "CD": "Africa",
        "CG": "Africa",
        "CK": "Oceania",
        "CO": "South America",
        "KM": "Africa",
        "CV": "Africa",
        "CR": "North America",
        "CU": "North America",
        "CW": "North America",
        "CX": "Asia",
        "KY": "North America",
        "CY": "Asia",
        "CZ": "Europe",
        "DE": "Europe",
        "DJ": "Africa",
        "DM": "North America",
        "DK": "Europe",
        "DO": "North America",
        "DZ": "Africa",
        "EC": "South America",
        "EG": "Africa",
        "ER": "Africa",
        "ES": "Europe",
        "EE": "Europe",
        "ET": "Africa",
        "FI": "Europe",
        "FJ": "Oceania",
        "FK": "South America",
        "FR": "Europe",
        "FO": "Europe",
        "FM": "Oceania",
        "GA": "Africa",
        "GB": "Europe",
        "GE": "Asia",
        "GG": "Europe",
        "GH": "Africa",
        "GI": "Europe",
        "GN": "Africa",
        "GP": "North America",
        "GM": "Africa",
        "GW": "Africa",
        "GQ": "Africa",
        "GR": "Europe",
        "GD": "North America",
        "GL": "North America",
        "GT": "North America",
        "GF": "South America",
        "GU": "Oceania",
        "GY": "South America",
        "HK": "Asia",
        "HM": "Antarctica",
        "HN": "North America",
        "HR": "Europe",
        "HT": "North America",
        "HU": "Europe",
        "ID": "Asia",
        "IM": "Europe",
        "IN": "Asia",
        "IO": "Asia",
        "IE": "Europe",
        "IR": "Asia",
        "IQ": "Asia",
        "IS": "Europe",
        "IL": "Asia",
        "IT": "Europe",
        "JM": "North America",
        "JE": "Europe",
        "JO": "Asia",
        "JP": "Asia",
        "KZ": "Asia",
        "KE": "Africa",
        "KG": "Asia",
        "KH": "Asia",
        "KI": "Oceania",
        "KN": "North America",
        "KR": "Asia",
        "KW": "Asia",
        "LA": "Asia",
        "LB": "Asia",
        "LR": "Africa",
        "LY": "Africa",
        "LC": "North America",
        "LI": "Europe",
        "LK": "Asia",
        "LS": "Africa",
        "LT": "Europe",
        "LU": "Europe",
        "LV": "Europe",
        "MO": "Asia",
        "MF": "North America",
        "MA": "Africa",
        "MC": "Europe",
        "MD": "Europe",
        "MG": "Africa",
        "MV": "Asia",
        "MX": "North America",
        "MH": "Oceania",
        "MK": "Europe",
        "ML": "Africa",
        "MT": "Europe",
        "MM": "Asia",
        "ME": "Europe",
        "MN": "Asia",
        "MP": "Oceania",
        "MZ": "Africa",
        "MR": "Africa",
        "MS": "North America",
        "MQ": "North America",
        "MU": "Africa",
        "MW": "Africa",
        "MY": "Asia",
        "YT": "Africa",
        "NA": "Africa",
        "NC": "Oceania",
        "NE": "Africa",
        "NF": "Oceania",
        "NG": "Africa",
        "NI": "North America",
        "NU": "Oceania",
        "NL": "Europe",
        "NO": "Europe",
        "NP": "Asia",
        "NR": "Oceania",
        "NZ": "Oceania",
        "OM": "Asia",
        "PK": "Asia",
        "PA": "North America",
        "PE": "South America",
        "PH": "Asia",
        "PW": "Oceania",
        "PG": "Oceania",
        "PL": "Europe",
        "PR": "North America",
        "KP": "Asia",
        "PT": "Europe",
        "PY": "South America",
        "PS": "Asia",
        "PF": "Oceania",
        "QA": "Asia",
        "RE": "Africa",
        "RO": "Europe",
        "RU": "Europe",
        "RW": "Africa",
        "SA": "Asia",
        "SD": "Africa",
        "SN": "Africa",
        "SG": "Asia",
        "GS": "South America",
        "SH": "Africa",
        "SJ": "Europe",
        "SB": "Oceania",
        "SL": "Africa",
        "SV": "North America",
        "SM": "Europe",
        "SO": "Africa",
        "PM": "North America",
        "RS": "Europe",
        "SS": "Africa",
        "ST": "Africa",
        "SR": "South America",
        "SK": "Europe",
        "SI": "Europe",
        "SE": "Europe",
        "SZ": "Africa",
        "SC": "Africa",
        "SY": "Asia",
        "TC": "North America",
        "TD": "Africa",
        "TG": "Africa",
        "TH": "Asia",
        "TJ": "Asia",
        "TK": "Oceania",
        "TM": "Asia",
        "TO": "Oceania",
        "TT": "North America",
        "TN": "Africa",
        "TR": "Asia",
        "TV": "Oceania",
        "TW": "Asia",
        "TZ": "Africa",
        "UG": "Africa",
        "UA": "Europe",
        "UY": "South America",
        "US": "North America",
        "UZ": "Asia",
        "VC": "North America",
        "VE": "South America",
        "VG": "North America",
        "VI": "North America",
        "VN": "Asia",
        "VU": "Oceania",
        "WF": "Oceania",
        "WS": "Oceania",
        "YE": "Asia",
        "ZA": "Africa",
        "ZM": "Africa",
        "ZW": "Africa",
    }


def show_country_coverage(
    cache_dir: Optional[str] = None,
    show_missing: bool = False,
    return_data: bool = False,
    check_live_counts: bool = False,
    countries_to_check: Optional[list[str]] = None,
    show_outdated_only: bool = False,
    outdated_threshold: float = 0.95,
) -> Optional[dict[str, any]]:
    """
    Show countries present in cache with element counts and percentages.

    This function analyzes the OSM cache to show which countries have been cached,
    along with statistics about the number of plants and generators per country.
    Can optionally compare with live Overpass API counts.

    Parameters
    ----------
    cache_dir : Optional[str], default None
        Cache directory path. If None, uses powerplantmatching default.
    show_missing : bool, default False
        Also show countries that are missing from the cache.
    return_data : bool, default False
        If True, return the coverage data as a dictionary instead of printing.
    check_live_counts : bool, default False
        Whether to check current counts from Overpass API for comparison.
    countries_to_check : Optional[list[str]], default None
        Specific countries to check live counts for. If None, checks all cached countries.
    show_outdated_only : bool, default False
        Only show countries where cache appears outdated.
    outdated_threshold : float, default 0.95
        Ratio threshold - cache is considered outdated if cached/live ratio is below this.

    Returns
    -------
    Optional[dict[str, any]]
        If return_data is True, returns a dictionary with coverage statistics.
        Otherwise returns None and prints the coverage report.

    Examples
    --------
    >>> from powerplantmatching.osm import show_country_coverage
    >>>
    >>> # Show basic coverage report
    >>> show_country_coverage()
    >>>
    >>> # Show coverage with live comparison
    >>> show_country_coverage(check_live_counts=True)
    >>>
    >>> # Get coverage data as dictionary
    >>> coverage_data = show_country_coverage(return_data=True)
    >>> print(f"Countries in cache: {coverage_data['countries_cached']}")
    """
    # Get configuration
    config = get_config()
    osm_config = config.get("OSM", {})

    # Set up cache directory
    if cache_dir is None:
        fn = _data_in(osm_config.get("fn", "osm_data.csv"))
        cache_dir = os.path.join(os.path.dirname(fn), "osm_cache")

    # Initialize cache
    cache = ElementCache(cache_dir)

    if not return_data:
        print("Loading cache data...")

    cache.load_all_caches()

    # Get all possible countries
    all_countries = {c.alpha_2: c.name for c in pycountry.countries}
    total_possible_countries = len(all_countries)

    # Analyze cached data
    cached_countries = {}
    total_plants = 0
    total_generators = 0

    # Get plants data
    for country_code, data in cache.plants_cache.items():
        if country_code not in cached_countries:
            cached_countries[country_code] = {
                "name": all_countries.get(country_code, f"Unknown ({country_code})"),
                "plants_cached": 0,
                "generators_cached": 0,
                "total_cached": 0,
                "plants_live": None,
                "generators_live": None,
                "total_live": None,
                "plants_diff": None,
                "generators_diff": None,
                "cache_status": "unknown",
            }
        plants_count = len(data.get("elements", []))
        cached_countries[country_code]["plants_cached"] = plants_count
        total_plants += plants_count

    # Get generators data
    for country_code, data in cache.generators_cache.items():
        if country_code not in cached_countries:
            cached_countries[country_code] = {
                "name": all_countries.get(country_code, f"Unknown ({country_code})"),
                "plants_cached": 0,
                "generators_cached": 0,
                "total_cached": 0,
                "plants_live": None,
                "generators_live": None,
                "total_live": None,
                "plants_diff": None,
                "generators_diff": None,
                "cache_status": "unknown",
            }
        generators_count = len(data.get("elements", []))
        cached_countries[country_code]["generators_cached"] = generators_count
        total_generators += generators_count

    # Calculate cached totals
    for country_code, data in cached_countries.items():
        data["total_cached"] = data["plants_cached"] + data["generators_cached"]

    total_elements = total_plants + total_generators

    # Check live counts if requested
    if check_live_counts:
        # Initialize Overpass API client
        api_url = osm_config.get("overpass_api", {}).get(
            "url", "https://overpass-api.de/api/interpreter"
        )
        timeout = osm_config.get("overpass_api", {}).get("timeout", 300)
        max_retries = osm_config.get("overpass_api", {}).get("max_retries", 3)
        retry_delay = osm_config.get("overpass_api", {}).get("retry_delay", 5)

        if not return_data:
            print("\nChecking live counts from Overpass API...")

        with OverpassAPIClient(
            api_url=api_url,
            cache_dir=cache_dir,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            show_progress=False,
        ) as client:
            # Determine which countries to check
            countries_to_check_list = countries_to_check or list(
                cached_countries.keys()
            )

            # Use tqdm for progress if not returning data
            iterator = (
                tqdm(
                    countries_to_check_list, desc="Checking live counts", unit="country"
                )
                if not return_data
                else countries_to_check_list
            )

            for country_code in iterator:
                if country_code not in cached_countries:
                    continue

                country_name = cached_countries[country_code]["name"]

                try:
                    # Get live counts
                    live_counts = client.count_country_elements(country_name, "both")

                    # Update data with live counts
                    cached_countries[country_code]["plants_live"] = live_counts.get(
                        "plants", -1
                    )
                    cached_countries[country_code]["generators_live"] = live_counts.get(
                        "generators", -1
                    )

                    if (
                        live_counts.get("plants", -1) >= 0
                        and live_counts.get("generators", -1) >= 0
                    ):
                        cached_countries[country_code]["total_live"] = (
                            live_counts["plants"] + live_counts["generators"]
                        )

                        # Calculate differences
                        cached_countries[country_code]["plants_diff"] = (
                            cached_countries[country_code]["plants_cached"]
                            - live_counts["plants"]
                        )
                        cached_countries[country_code]["generators_diff"] = (
                            cached_countries[country_code]["generators_cached"]
                            - live_counts["generators"]
                        )

                        # Determine cache status
                        plants_ratio = (
                            cached_countries[country_code]["plants_cached"]
                            / live_counts["plants"]
                            if live_counts["plants"] > 0
                            else 1.0
                        )
                        generators_ratio = (
                            cached_countries[country_code]["generators_cached"]
                            / live_counts["generators"]
                            if live_counts["generators"] > 0
                            else 1.0
                        )

                        if (
                            plants_ratio < outdated_threshold
                            or generators_ratio < outdated_threshold
                        ):
                            cached_countries[country_code]["cache_status"] = "outdated"
                        elif plants_ratio > 1.0 or generators_ratio > 1.0:
                            cached_countries[country_code]["cache_status"] = "ahead"
                        else:
                            cached_countries[country_code]["cache_status"] = "current"
                    else:
                        cached_countries[country_code]["cache_status"] = "error"

                except Exception as e:
                    logger.error(
                        f"Error checking live counts for {country_name}: {str(e)}"
                    )
                    cached_countries[country_code]["cache_status"] = "error"

    # Prepare return data if requested
    if return_data:
        continent_map = get_continent_mapping()
        continent_stats = {}

        for code, data in cached_countries.items():
            continent = continent_map.get(code, "Unknown")
            if continent not in continent_stats:
                continent_stats[continent] = {
                    "countries": 0,
                    "plants_cached": 0,
                    "generators_cached": 0,
                    "total_cached": 0,
                    "outdated_countries": 0,
                }
            continent_stats[continent]["countries"] += 1
            continent_stats[continent]["plants_cached"] += data["plants_cached"]
            continent_stats[continent]["generators_cached"] += data["generators_cached"]
            continent_stats[continent]["total_cached"] += data["total_cached"]
            if data.get("cache_status") == "outdated":
                continent_stats[continent]["outdated_countries"] += 1

        missing_countries = []
        for code, name in all_countries.items():
            if code not in cached_countries:
                missing_countries.append({"name": name, "code": code})

        result = {
            "cache_dir": cache_dir,
            "countries_cached": len(cached_countries),
            "countries_total": total_possible_countries,
            "coverage_percentage": len(cached_countries)
            / total_possible_countries
            * 100,
            "total_elements": total_elements,
            "total_plants": total_plants,
            "total_generators": total_generators,
            "cached_countries": cached_countries,
            "missing_countries": missing_countries,
            "continent_stats": continent_stats,
        }

        if check_live_counts:
            result["timestamp"] = datetime.now(timezone.utc).isoformat()

        return result

    # Print results
    print("\n" + "=" * (100 if check_live_counts else 80))
    print(
        "OSM CACHE COUNTRY COVERAGE"
        + (" WITH LIVE COMPARISON" if check_live_counts else "")
    )
    print("=" * (100 if check_live_counts else 80))
    print(
        f"\nCountries in cache: {len(cached_countries)} / {total_possible_countries} "
        f"({len(cached_countries) / total_possible_countries * 100:.1f}%)"
    )
    print(
        "Total "
        + ("cached " if check_live_counts else "")
        + f"elements: {total_elements:,} (Plants: {total_plants:,}, Generators: {total_generators:,})"
    )

    # Count status types if checking live counts
    if check_live_counts:
        status_counts = {
            "current": 0,
            "outdated": 0,
            "ahead": 0,
            "error": 0,
            "unknown": 0,
        }
        for data in cached_countries.values():
            status_counts[data["cache_status"]] += 1

        print("\nCache Status Summary:")
        print(f"  ✓ Current: {status_counts['current']} countries")
        print(f"  ⚠ Outdated: {status_counts['outdated']} countries")
        print(f"  ⟳ Ahead: {status_counts['ahead']} countries")
        print(f"  ✗ Error: {status_counts['error']} countries")
        if status_counts["unknown"] > 0:
            print(f"  ? Unknown: {status_counts['unknown']} countries")

    # Create table data
    table_data = []
    for code, data in sorted(
        cached_countries.items(), key=lambda x: x[1]["total_cached"], reverse=True
    ):
        # Skip if showing outdated only and this isn't outdated
        if show_outdated_only and data.get("cache_status") != "outdated":
            continue

        if check_live_counts:
            row = [
                data["name"][:20],  # Truncate long names
                code,
                f"{data['plants_cached']:,}",
                f"{data['generators_cached']:,}",
                f"{data['total_cached']:,}",
            ]

            # Add live counts
            row.extend(
                [
                    f"{data['plants_live']:,}"
                    if data["plants_live"] is not None and data["plants_live"] >= 0
                    else "ERR"
                    if data["plants_live"] is not None
                    else "?",
                    f"{data['generators_live']:,}"
                    if data["generators_live"] is not None
                    and data["generators_live"] >= 0
                    else "ERR"
                    if data["generators_live"] is not None
                    else "?",
                    f"{data['total_live']:,}"
                    if data["total_live"] is not None
                    else "?",
                ]
            )

            # Add differences
            if data["plants_diff"] is not None:
                plants_diff_str = f"{data['plants_diff']:+,}"
                generators_diff_str = f"{data['generators_diff']:+,}"
            else:
                plants_diff_str = "?"
                generators_diff_str = "?"

            row.extend([plants_diff_str, generators_diff_str])

            # Add status
            status_symbols = {
                "current": "✓",
                "outdated": "⚠",
                "ahead": "⟳",
                "error": "✗",
                "unknown": "?",
            }
            row.append(status_symbols.get(data["cache_status"], "?"))
        else:
            # Basic coverage without live counts
            plants_pct = (
                (data["plants_cached"] / total_plants * 100) if total_plants > 0 else 0
            )
            generators_pct = (
                (data["generators_cached"] / total_generators * 100)
                if total_generators > 0
                else 0
            )
            total_pct = (
                (data["total_cached"] / total_elements * 100)
                if total_elements > 0
                else 0
            )

            row = [
                data["name"],
                code,
                f"{data['plants_cached']:,}",
                f"{plants_pct:.1f}%",
                f"{data['generators_cached']:,}",
                f"{generators_pct:.1f}%",
                f"{data['total_cached']:,}",
                f"{total_pct:.1f}%",
            ]

        table_data.append(row)

    # Print table
    if table_data:
        if show_outdated_only:
            print("\nOUTDATED COUNTRIES:")
        else:
            print(
                "\n"
                + ("COUNTRY DETAILS:" if check_live_counts else "CACHED COUNTRIES:")
            )

        if check_live_counts:
            headers = [
                "Country",
                "Code",
                "Plants(C)",
                "Gen(C)",
                "Total(C)",
                "Plants(L)",
                "Gen(L)",
                "Total(L)",
                "P.Diff",
                "G.Diff",
                "Status",
            ]
        else:
            headers = [
                "Country",
                "Code",
                "Plants",
                "P%",
                "Generators",
                "G%",
                "Total",
                "T%",
            ]

        print(format_table(table_data, headers))

        if check_live_counts:
            print("\nLegend: (C)=Cached, (L)=Live, Diff=Cached-Live")
            print("Status: ✓=Current, ⚠=Outdated, ⟳=Ahead, ✗=Error, ?=Unknown")

    # Show missing countries if requested
    if show_missing and not show_outdated_only:
        missing_countries = []
        for code, name in all_countries.items():
            if code not in cached_countries:
                missing_countries.append((name, code))

        if missing_countries:
            print(f"\nMISSING COUNTRIES ({len(missing_countries)}):")
            missing_sorted = sorted(missing_countries, key=lambda x: x[0])

            # Print in columns
            cols = 3
            for i in range(0, len(missing_sorted), cols):
                row = missing_sorted[i : i + cols]
                print("  " + "  ".join(f"{name:<30} ({code})" for name, code in row))
        else:
            print("\nAll countries are cached!")

    # Print continent breakdown if not showing outdated only
    if not show_outdated_only:
        print("\nBREAKDOWN BY CONTINENT:")
        continent_map = get_continent_mapping()
        continent_stats = {}

        for code, data in cached_countries.items():
            continent = continent_map.get(code, "Unknown")
            if continent not in continent_stats:
                continent_stats[continent] = {
                    "countries": 0,
                    "plants": 0,
                    "generators": 0,
                    "total": 0,
                    "outdated": 0,
                }
            continent_stats[continent]["countries"] += 1
            continent_stats[continent]["plants"] += data["plants_cached"]
            continent_stats[continent]["generators"] += data["generators_cached"]
            continent_stats[continent]["total"] += data["total_cached"]
            if data.get("cache_status") == "outdated":
                continent_stats[continent]["outdated"] += 1

        continent_table = []
        for continent in [
            "Europe",
            "Asia",
            "Africa",
            "North America",
            "South America",
            "Oceania",
            "Antarctica",
            "Unknown",
        ]:
            if continent in continent_stats:
                stats = continent_stats[continent]
                row = [
                    continent,
                    stats["countries"],
                    f"{stats['plants']:,}",
                    f"{stats['generators']:,}",
                    f"{stats['total']:,}",
                    f"{stats['total'] / total_elements * 100:.1f}%"
                    if total_elements > 0
                    else "0.0%",
                ]
                if check_live_counts:
                    row.append(f"{stats['outdated']}")
                continent_table.append(row)

        headers = [
            "Continent",
            "Countries",
            "Plants",
            "Generators",
            "Total",
            "% of Total",
        ]
        if check_live_counts:
            headers.append("Outdated")
        print(format_table(continent_table, headers))


def find_outdated_caches(
    cache_dir: Optional[str] = None,
    threshold: float = 0.95,
    check_specific_countries: Optional[list[str]] = None,
) -> list[dict[str, any]]:
    """
    Find countries where the cache appears to be outdated.

    Parameters
    ----------
    cache_dir : Optional[str], default None
        Cache directory path. If None, uses powerplantmatching default.
    threshold : float, default 0.95
        Ratio threshold - cache is considered outdated if cached/live ratio is below this.
    check_specific_countries : Optional[list[str]], default None
        Specific countries to check. If None, checks all cached countries.

    Returns
    -------
    list[dict[str, any]]
        List of outdated countries with details about the discrepancy.

    Examples
    --------
    >>> from powerplantmatching.osm import find_outdated_caches
    >>>
    >>> # Find all outdated caches
    >>> outdated = find_outdated_caches()
    >>> for country in outdated:
    ...     print(f"{country['name']}: {country['total_missing']} new elements on OSM")
    """
    # Get coverage data with live comparison
    coverage_data = show_country_coverage(
        cache_dir=cache_dir,
        return_data=True,
        check_live_counts=True,
        countries_to_check=check_specific_countries,
        outdated_threshold=threshold,
    )

    outdated_countries = []

    for code, data in coverage_data["cached_countries"].items():
        if data.get("cache_status") == "outdated":
            outdated_info = {
                "code": code,
                "name": data["name"],
                "plants_cached": data["plants_cached"],
                "plants_live": data["plants_live"],
                "plants_missing": data["plants_live"] - data["plants_cached"],
                "generators_cached": data["generators_cached"],
                "generators_live": data["generators_live"],
                "generators_missing": data["generators_live"]
                - data["generators_cached"],
                "total_missing": (
                    (data["plants_live"] - data["plants_cached"])
                    + (data["generators_live"] - data["generators_cached"])
                ),
                "cache_coverage_ratio": data["total_cached"] / data["total_live"]
                if data["total_live"] and data["total_live"] > 0
                else 0.0,
            }
            outdated_countries.append(outdated_info)

    # Sort by total missing elements
    outdated_countries.sort(key=lambda x: x["total_missing"], reverse=True)

    return outdated_countries
