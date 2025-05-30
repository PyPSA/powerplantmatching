import json
import logging
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

PROCESSING_PARAMETERS = [
    "capacity_extraction",
    "capacity_estimation",
    "units_clustering",
    "source_mapping",
    "technology_mapping",
    "source_technology_mapping",
    "plants_only",
    "missing_name_allowed",
    "missing_technology_allowed",
    "missing_start_date_allowed",
    "sources",
]


class RejectionReason(Enum):
    INVALID_ELEMENT_TYPE = "Invalid element type"
    COORDINATES_NOT_FOUND = "Could not determine coordinates"
    MISSING_TECHNOLOGY_TAG = "Missing technology tag"
    MISSING_TECHNOLOGY_TYPE = "Missing technology type"
    MISSING_SOURSE_TAG = "Missing source tag"
    MISSING_SOURCE_TYPE = "Missing source type"
    CAPACITY_PLACEHOLDER = "Capacity placeholder value"
    MISSING_OUTPUT_TAG = "Missing output tag"
    MISSING_NAME_TAG = "Missing name tag"
    CAPACITY_REGEX_NO_MATCH = "Capacity regex no match"
    ESTIMATION_METHOD_UNKNOWN = "Unknown estimation method"
    CAPACITY_DECIMAL_FORMAT = "Capacity decimal format"
    CAPACITY_REGEX_ERROR = "Capacity regex error"
    CAPACITY_NON_NUMERIC = "Capacity non-numeric"
    CAPACITY_UNSUPPORTED_UNIT = "Unsupported capacity unit"
    CAPACITY_ZERO = "Capacity zero"
    ELEMENT_ALREADY_PROCESSED = "Element already processed"
    WITHIN_EXISTING_PLANT = "Element within existing plant geometry"
    INVALID_START_DATE_FORMAT = "Invalid start date format"
    MISSING_START_DATE_TAG = "Missing start date tag"
    OTHER = "Other reason"


class ElementType(Enum):
    NODE = "node"
    WAY = "way"
    RELATION = "relation"


@dataclass
class Unit:
    # Using PowerPlantMatching column names directly
    projectID: str
    Country: str | None = None
    lat: float | None = None
    lon: float | None = None
    type: str | None = None
    Fueltype: str | None = None
    Technology: str | None = None
    Capacity: float | None = None
    Name: str | None = None
    generator_count: int | None = None
    Set: str | None = None
    capacity_source: str | None = None
    DateIn: str | None = None
    id: str | None = None

    # Metadata fields for caching
    created_at: str | None = None
    config_hash: str | None = None
    config_version: str | None = None
    processing_parameters: dict | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    def is_valid_for_config(self, current_config: dict) -> bool:
        """Check if this unit is valid for the current configuration."""
        if not self.config_hash:
            return False

        # Generate hash of relevant parts of current config
        current_hash = self._generate_config_hash(current_config)
        return current_hash == self.config_hash

    @staticmethod
    def _generate_config_hash(config: dict) -> str:
        """Generate a hash from configuration parameters that affect processing."""
        import hashlib
        import json

        # Create a subset of the config with only the relevant keys
        relevant_config = {
            k: config.get(k) for k in PROCESSING_PARAMETERS if k in config
        }

        # Generate a hash
        config_str = json.dumps(relevant_config, sort_keys=True, indent=4)
        return hashlib.md5(config_str.encode()).hexdigest()


@dataclass
class PlantPolygon:
    id: str
    type: str
    geometry: Any  # This would be a shapely Polygon in practice


class Units:
    """Collection class for managing multiple Unit objects with GeoJSON export capabilities"""

    def __init__(self, units: list[Unit] | None = None):
        """
        Initialize Units collection

        Parameters
        ----------
        units : list[Unit] | None
            Initial list of units
        """
        self.units: list[Unit] = units or []

    def add_unit(self, unit: Unit) -> None:
        """Add a single unit to the collection"""
        self.units.append(unit)

    def add_units(self, units: list[Unit]) -> None:
        """Add multiple units to the collection"""
        self.units.extend(units)

    def __len__(self) -> int:
        """Return number of units in collection"""
        return len(self.units)

    def __iter__(self):
        """Make collection iterable"""
        return iter(self.units)

    def __getitem__(self, index):
        """Allow indexing"""
        return self.units[index]

    def filter_by_country(self, country: str) -> "Units":
        """Filter units by country"""
        filtered = [unit for unit in self.units if unit.Country == country]
        return Units(filtered)

    def filter_by_fueltype(self, fueltype: str) -> "Units":
        """Filter units by fuel type"""
        filtered = [unit for unit in self.units if unit.Fueltype == fueltype]
        return Units(filtered)

    def filter_by_technology(self, technology: str) -> "Units":
        """Filter units by technology"""
        filtered = [unit for unit in self.units if unit.Technology == technology]
        return Units(filtered)

    def get_statistics(self) -> dict[str, Any]:
        """Get basic statistics about the units collection"""
        if not self.units:
            return {"total_units": 0}

        # Count valid coordinates
        units_with_coords = [
            u for u in self.units if u.lat is not None and u.lon is not None
        ]

        # Get countries
        countries = set(u.Country for u in self.units if u.Country)

        # Get fuel types
        fueltypes = set(u.Fueltype for u in self.units if u.Fueltype)

        # Get technologies
        technologies = set(u.Technology for u in self.units if u.Technology)

        # Calculate total capacity
        total_capacity = sum(u.Capacity for u in self.units if u.Capacity is not None)

        return {
            "total_units": len(self.units),
            "units_with_coordinates": len(units_with_coords),
            "coverage_percentage": round(
                len(units_with_coords) / len(self.units) * 100, 1
            ),
            "countries": sorted(list(countries)),
            "fuel_types": sorted(list(fueltypes)),
            "technologies": sorted(list(technologies)),
            "total_capacity_mw": round(total_capacity, 2) if total_capacity else 0,
            "average_capacity_mw": round(total_capacity / len(self.units), 2)
            if total_capacity and self.units
            else 0,
        }

    def generate_geojson_report(self) -> dict[str, Any]:
        """
        Generate a clean GeoJSON report of all units with coordinates

        Returns
        -------
        dict[str, Any]
            GeoJSON FeatureCollection containing power plant units with clean structure
        """
        features = []

        for unit in self.units:
            # Skip units without coordinates
            if unit.lat is None or unit.lon is None:
                continue

            # Create clean feature with only essential power plant data
            properties = {}

            # Add non-null unit properties (excluding metadata fields)
            if unit.Name:
                properties["name"] = unit.Name
            if unit.projectID:
                properties["project_id"] = unit.projectID
            if unit.Country:
                properties["country"] = unit.Country
            if unit.Fueltype:
                properties["fuel_type"] = unit.Fueltype
            if unit.Technology:
                properties["technology"] = unit.Technology
            if unit.Capacity is not None:
                properties["capacity_mw"] = unit.Capacity
            if unit.capacity_source:
                properties["capacity_source"] = unit.capacity_source
            if unit.DateIn:
                properties["date_in"] = unit.DateIn
            if unit.type:
                properties["plant_type"] = unit.type
            if unit.Set:
                properties["set_type"] = unit.Set
            if unit.id:
                properties["osm_id"] = unit.id
            if unit.generator_count is not None:
                properties["generator_count"] = unit.generator_count

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        unit.lon,
                        unit.lat,
                    ],  # GeoJSON uses [lon, lat] order
                },
                "properties": properties,
            }
            features.append(feature)

        # Create clean GeoJSON structure without nested statistics
        geojson = {"type": "FeatureCollection", "features": features}

        logger.info(
            f"Generated GeoJSON report with {len(features)} power plant features from {len(self.units)} total units"
        )
        return geojson

    def generate_styled_geojson_report(self) -> dict[str, Any]:
        """
        Generate a styled GeoJSON report optimized for web mapping applications

        Returns
        -------
        dict[str, Any]
            GeoJSON FeatureCollection with styling properties for web maps
        """
        features = []

        for unit in self.units:
            # Skip units without coordinates
            if unit.lat is None or unit.lon is None:
                continue

            # Determine marker properties based on fuel type
            fueltype_colors = {
                "Solar": "#FFD700",  # Gold
                "Wind": "#87CEEB",  # Sky Blue
                "Hydro": "#00CED1",  # Dark Turquoise
                "Nuclear": "#9370DB",  # Medium Purple
                "Natural Gas": "#FF6347",  # Tomato
                "Hard Coal": "#2F4F4F",  # Dark Slate Gray
                "Oil": "#000000",  # Black
                "Biogas": "#9ACD32",  # Yellow Green
                "Solid Biomass": "#228B22",  # Forest Green
                "Geothermal": "#DAA520",  # Goldenrod
                "Waste": "#808000",  # Olive
                "Other": "#D3D3D3",  # Light Gray
            }

            # Get color for fuel type
            color = fueltype_colors.get(unit.Fueltype, "#808080")  # Default gray

            # Determine marker size based on capacity
            if unit.Capacity:
                if unit.Capacity >= 1000:
                    marker_size = "large"
                    radius = 12
                elif unit.Capacity >= 100:
                    marker_size = "medium"
                    radius = 8
                else:
                    marker_size = "small"
                    radius = 5
            else:
                marker_size = "unknown"
                radius = 6

            # Create clean properties with styling
            properties = {
                # Core data
                "name": unit.Name or f"Unnamed {unit.Fueltype or 'Power'} Plant",
                "fuel_type": unit.Fueltype,
                "technology": unit.Technology,
                "capacity_mw": unit.Capacity,
                "country": unit.Country,
                # Styling properties for mapping
                "marker-color": color,
                "marker-size": marker_size,
                "marker-symbol": self._get_fuel_symbol(unit.Fueltype),
                "stroke": "#555555",
                "stroke-width": 2,
                "fill": color,
                "fill-opacity": 0.7,
                "radius": radius,
            }

            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [unit.lon, unit.lat]},
                "properties": properties,
            }
            features.append(feature)

        geojson = {"type": "FeatureCollection", "features": features}

        logger.info(f"Generated styled GeoJSON report with {len(features)} features")
        return geojson

    def _get_fuel_symbol(self, fueltype: str | None) -> str:
        """Get map symbol for fuel type"""
        fuel_symbols = {
            "Solar": "solar",
            "Wind": "wind",
            "Hydro": "water",
            "Nuclear": "nuclear",
            "Natural Gas": "gas-station",
            "Hard Coal": "mine",
            "Oil": "fuel",
            "Biogas": "leaf",
            "Solid Biomass": "tree",
            "Geothermal": "volcano",
            "Waste": "waste-basket",
            "Other": "circle",
        }
        return fuel_symbols.get(fueltype, "industrial")

    def save_geojson_report(self, filepath: str, styled: bool = False) -> None:
        """
        Generate and save a GeoJSON report to file

        Parameters
        ----------
        filepath : str
            Path to save the GeoJSON file
        styled : bool, default False
            If True, include styling properties for web mapping
        """
        if styled:
            geojson_data = self.generate_styled_geojson_report()
        else:
            geojson_data = self.generate_geojson_report()

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(geojson_data, f, indent=2, ensure_ascii=False)

        stats = self.get_statistics()
        report_type = "styled" if styled else "clean"
        logger.info(f"Saved {report_type} GeoJSON report to {filepath}")
        logger.info(
            f"Report contains {stats['units_with_coordinates']} units with coordinates out of {stats['total_units']} total units"
        )

    def to_dataframe(self) -> pd.DataFrame:
        """Convert units collection to pandas DataFrame"""

        if not self.units:
            return pd.DataFrame()

        units_dicts = [unit.to_dict() for unit in self.units]
        return pd.DataFrame(units_dicts)

    def save_csv(self, filepath: str) -> None:
        """Save units collection as CSV file"""
        df = self.to_dataframe()
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(self.units)} units to CSV: {filepath}")
