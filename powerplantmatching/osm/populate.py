"""
Cache population utilities for OSM module.

This module provides functions to populate the OSM cache with data from all countries.
"""

import logging
import os
import time
from datetime import timedelta
from typing import Optional, Union

import pycountry
from tqdm import tqdm

from powerplantmatching.core import _data_in, get_config

from .client import OverpassAPIClient
from .coverage import get_continent_mapping

logger = logging.getLogger(__name__)


def get_all_countries(sort_by_continent: bool = True) -> list[dict[str, str]]:
    """
    Get list of all countries with their codes.

    Parameters
    ----------
    sort_by_continent : bool, default True
        If True, sort countries by continent first, then alphabetically.
        If False, sort alphabetically by country name.

    Returns
    -------
    list[dict[str, str]]
        list of country dictionaries with keys: name, code, alpha3, continent
    """
    continent_map = get_continent_mapping()
    countries = []

    for country in pycountry.countries:
        country_data = {
            "name": country.name,
            "code": country.alpha_2,
            "alpha3": country.alpha_3,
            "continent": continent_map.get(country.alpha_2, "Unknown"),
        }
        countries.append(country_data)

    if sort_by_continent:
        # Define continent order
        continent_order = [
            "Europe",
            "Asia",
            "Africa",
            "North America",
            "South America",
            "Oceania",
            "Antarctica",
            "Unknown",
        ]

        # Sort by continent first, then by country name within each continent
        countries.sort(key=lambda x: (continent_order.index(x["continent"]), x["name"]))
    else:
        # Sort alphabetically by name
        countries.sort(key=lambda x: x["name"])

    return countries


def populate_cache(
    countries: Optional[Union[str, list[str]]] = None,
    force_refresh: bool = False,
    plants_only: bool = False,
    cache_dir: Optional[str] = None,
    dry_run: bool = False,
    show_progress: bool = True,
    sort_by_continent: bool = True,
) -> dict[str, any]:
    """
    Populate OSM cache with raw data for specified countries.

    This function downloads OSM power plant and generator data for the specified
    countries and stores it in the cache without any processing or unit creation.

    Parameters
    ----------
    countries : Optional[Union[str, list[str]]], default None
        Countries to download. Can be:
        - None: Download all countries
        - str: Single country name or code (e.g., "Germany" or "DE")
        - list[str]: list of country names or codes
    force_refresh : bool, default False
        Force download even if data already exists in cache.
    plants_only : bool, default False
        Only download plants, skip generators.
    cache_dir : Optional[str], default None
        Override cache directory. If None, uses powerplantmatching default.
    dry_run : bool, default False
        Show what would be downloaded without actually downloading.
    show_progress : bool, default True
        Show progress bars during download.
    sort_by_continent : bool, default True
        Sort countries by continent when downloading all countries.

    Returns
    -------
    dict[str, any]
        Summary of the cache population operation with keys:
        - total_countries: Total number of countries processed
        - succeeded: Number of successful downloads
        - failed: Number of failed downloads
        - skipped: Number of skipped countries (already cached)
        - elapsed_time: Total time in seconds
        - failed_countries: list of failed countries with error details

    Examples
    --------
    >>> from powerplantmatching.osm import populate_cache
    >>>
    >>> # Download all countries
    >>> result = populate_cache()
    >>>
    >>> # Download specific countries
    >>> result = populate_cache(countries=["Germany", "France", "Spain"])
    >>>
    >>> # Force refresh even if cached
    >>> result = populate_cache(countries="Germany", force_refresh=True)
    >>>
    >>> # Download only plants for all European countries
    >>> from powerplantmatching.osm import get_continent_mapping
    >>> continent_map = get_continent_mapping()
    >>> european_countries = [code for code, cont in continent_map.items() if cont == "Europe"]
    >>> result = populate_cache(countries=european_countries, plants_only=True)
    """
    # Get configuration
    config = get_config()
    osm_config = config.get("OSM", {})

    # Set up cache directory
    if cache_dir is None:
        fn = _data_in(osm_config.get("fn", "osm_data.csv"))
        cache_dir = os.path.join(os.path.dirname(fn), "osm_cache")

    # Get country list
    if countries is None:
        # All countries
        all_countries = get_all_countries(sort_by_continent=sort_by_continent)
    else:
        # Specific countries requested
        if isinstance(countries, str):
            countries = [countries]

        continent_map = get_continent_mapping()
        all_countries = []

        for name in countries:
            try:
                country = pycountry.countries.lookup(name)
                all_countries.append(
                    {
                        "name": country.name,
                        "code": country.alpha_2,
                        "alpha3": country.alpha_3,
                        "continent": continent_map.get(country.alpha_2, "Unknown"),
                    }
                )
            except LookupError:
                logger.warning(f"Country '{name}' not found, skipping")

    logger.info(f"Total countries to process: {len(all_countries)}")

    if dry_run:
        print("\nDry run - would download the following countries:")
        current_continent = None
        for country in all_countries:
            # Print continent header if it changes
            if sort_by_continent and country.get("continent") != current_continent:
                current_continent = country.get("continent", "Unknown")
                print(f"\n{current_continent}:")
            print(f"  {country['name']} ({country['code']})")

        return {
            "total_countries": len(all_countries),
            "dry_run": True,
            "countries": [c["name"] for c in all_countries],
        }

    # Create Overpass API client
    api_url = osm_config.get("overpass_api", {}).get(
        "url", "https://overpass-api.de/api/interpreter"
    )
    timeout = osm_config.get("overpass_api", {}).get("timeout", 300)
    max_retries = osm_config.get("overpass_api", {}).get("max_retries", 3)
    retry_delay = osm_config.get("overpass_api", {}).get("retry_delay", 5)

    if show_progress:
        print(f"Starting download of {len(all_countries)} countries...")
        print("-" * 60)

    # Statistics
    start_time = time.time()
    succeeded = 0
    failed = 0
    skipped = 0
    failed_countries = []

    try:
        with OverpassAPIClient(
            api_url=api_url,
            cache_dir=cache_dir,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            show_progress=show_progress,
        ) as client:
            # Use tqdm for progress if requested
            iterator = (
                tqdm(all_countries, desc="Downloading countries", unit="country")
                if show_progress
                else all_countries
            )
            current_continent = None

            for i, country in enumerate(iterator):
                country_name = country["name"]
                country_code = country["code"]
                country_continent = country.get("continent", "Unknown")

                # Update progress bar description when continent changes
                if (
                    show_progress
                    and sort_by_continent
                    and country_continent != current_continent
                ):
                    current_continent = country_continent
                    iterator.set_description(f"{current_continent}")

                try:
                    # Check if we already have data (unless force refresh)
                    has_plants = country_code in client.cache.plants_cache
                    has_generators = (
                        country_code in client.cache.generators_cache
                        if not plants_only
                        else True
                    )

                    if not force_refresh and has_plants and has_generators:
                        logger.info(f"Skipping {country_name} - already in cache")
                        skipped += 1
                    else:
                        logger.info(f"Downloading {country_name} ({country_code})")

                        # Use the client's get_country_data method
                        plants_data, generators_data = client.get_country_data(
                            country_name,
                            force_refresh=force_refresh,
                            plants_only=plants_only,
                        )

                        # The data is automatically cached by the client
                        plants_count = len(plants_data.get("elements", []))
                        generators_count = len(generators_data.get("elements", []))

                        logger.info(
                            f"Downloaded {country_name}: "
                            f"{plants_count} plants, {generators_count} generators"
                        )
                        succeeded += 1

                except Exception as e:
                    logger.error(f"Failed to download {country_name}: {str(e)}")
                    failed += 1
                    failed_countries.append(
                        {
                            "country": country_name,
                            "code": country_code,
                            "error": str(e),
                        }
                    )

                # Update progress bar if using tqdm
                if show_progress:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    remaining = len(all_countries) - (i + 1)
                    eta = remaining / rate if rate > 0 else 0

                    iterator.set_postfix(
                        {
                            "OK": succeeded,
                            "Skip": skipped,
                            "Fail": failed,
                            "ETA": str(timedelta(seconds=int(eta))),
                        }
                    )

            # Close progress bar if using tqdm
            if show_progress and hasattr(iterator, "close"):
                iterator.close()

    except Exception as e:
        logger.error(f"Fatal error during cache population: {str(e)}", exc_info=True)
        raise

    # Calculate final statistics
    total_time = time.time() - start_time

    # Log summary
    logger.info("\n" + "=" * 60)
    logger.info("CACHE POPULATION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total countries: {len(all_countries)}")
    logger.info(f"  ✓ Downloaded: {succeeded}")
    logger.info(f"  ⟳ Skipped (cached): {skipped}")
    logger.info(f"  ✗ Failed: {failed}")
    logger.info(f"Total time: {str(timedelta(seconds=int(total_time)))}")
    logger.info(f"Cache directory: {cache_dir}")
    logger.info("=" * 60)

    # Print summary if show_progress
    if show_progress:
        print("\n" + "=" * 60)
        print("CACHE POPULATION COMPLETE")
        print("=" * 60)
        print(f"Total countries: {len(all_countries)}")
        print(f"  ✓ Downloaded: {succeeded}")
        print(f"  ⟳ Skipped (cached): {skipped}")
        print(f"  ✗ Failed: {failed}")
        print(f"Total time: {str(timedelta(seconds=int(total_time)))}")
        print(f"Cache directory: {cache_dir}")
        print("=" * 60)

        # If there were failures, print them
        if failed > 0:
            print(f"\nFailed countries ({failed}):")
            for fc in failed_countries:
                print(f"  - {fc['country']} ({fc['code']}): {fc['error']}")

    return {
        "total_countries": len(all_countries),
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "elapsed_time": total_time,
        "failed_countries": failed_countries,
        "cache_dir": cache_dir,
    }
