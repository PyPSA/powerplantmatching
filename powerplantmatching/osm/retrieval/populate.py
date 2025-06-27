import logging
import time
from datetime import timedelta
from typing import Any, Optional, Union

import pycountry
from tqdm import tqdm

from powerplantmatching.core import get_config
from powerplantmatching.osm.quality.coverage import get_continent_mapping
from powerplantmatching.osm.utils import get_osm_cache_paths

from .client import OverpassAPIClient

logger = logging.getLogger(__name__)


def get_all_countries(sort_by_continent: bool = True) -> list[dict[str, str]]:
    continent_map = get_continent_mapping()
    countries = []

    for country in pycountry.countries:
        country_data = {
            "name": country.name,  # type: ignore
            "code": country.alpha_2,  # type: ignore
            "alpha3": country.alpha_3,  # type: ignore
            "continent": continent_map.get(country.alpha_2, "Unknown"),  # type: ignore
        }
        countries.append(country_data)

    if sort_by_continent:
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

        countries.sort(key=lambda x: (continent_order.index(x["continent"]), x["name"]))
    else:
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
) -> dict[str, Any]:
    config = get_config()
    osm_config = config.get("OSM", {})

    if cache_dir is None:
        cache_dir, _ = get_osm_cache_paths(config)

    if countries is None:
        all_countries = get_all_countries(sort_by_continent=sort_by_continent)
    else:
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
            if sort_by_continent and country.get("continent") != current_continent:
                current_continent = country.get("continent", "Unknown")
                print(f"\n{current_continent}:")
            print(f"  {country['name']} ({country['code']})")

        return {
            "total_countries": len(all_countries),
            "dry_run": True,
            "countries": [c["name"] for c in all_countries],
        }

    api_url = osm_config.get("overpass_api", {}).get(
        "url", "https://overpass-api.de/api/interpreter"
    )
    timeout = osm_config.get("overpass_api", {}).get("timeout", 300)
    max_retries = osm_config.get("overpass_api", {}).get("max_retries", 3)
    retry_delay = osm_config.get("overpass_api", {}).get("retry_delay", 5)

    if show_progress:
        print(f"Starting download of {len(all_countries)} countries...")
        print("-" * 60)

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

                if (
                    show_progress
                    and sort_by_continent
                    and country_continent != current_continent
                ):
                    current_continent = country_continent
                    if show_progress and hasattr(iterator, "set_description"):
                        iterator.set_description(f"{current_continent}")  # type: ignore

                try:
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

                        plants_data, generators_data = client.get_country_data(
                            country_name,
                            force_refresh=force_refresh,
                            plants_only=plants_only,
                        )

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

                if show_progress and hasattr(iterator, "set_postfix"):
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    remaining = len(all_countries) - (i + 1)
                    eta = remaining / rate if rate > 0 else 0

                    iterator.set_postfix(  # type: ignore
                        {
                            "OK": succeeded,
                            "Skip": skipped,
                            "Fail": failed,
                            "ETA": str(timedelta(seconds=int(eta))),
                        }
                    )

            # tqdm automatically closes when used as context manager
            # No need to manually close

    except Exception as e:
        logger.error(f"Fatal error during cache population: {str(e)}", exc_info=True)
        raise

    total_time = time.time() - start_time

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
