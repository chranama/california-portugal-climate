import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict

import yaml

from climate_pipeline.utils.open_meteo_client import OpenMeteoClient


def setup_logging(log_path: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    # Also log to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(console)


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    """
    CLI entrypoint for geocoding cities using the Open-Meteo Geocoding API.

    - Reads config from src/config/settings.yaml and src/config/cities.yaml
    - Calls the geocoding API for each configured city
    - Writes raw geocoding JSON into data/raw/geocoding/
    - Writes dbt/seeds/dim_city.csv with resolved city metadata
    """
    settings = load_yaml("src/config/settings.yaml")
    cities_cfg = load_yaml("src/config/cities.yaml")

    log_path = settings["logging"]["ingestion_log"]
    setup_logging(log_path)
    logger = logging.getLogger("geocoding")

    geocoding_dir = Path(settings["data"]["raw_geocoding_dir"])
    geocoding_dir.mkdir(parents=True, exist_ok=True)

    dim_city_csv_path = Path("dbt/seeds/dim_city.csv")
    dim_city_csv_path.parent.mkdir(parents=True, exist_ok=True)

    client = OpenMeteoClient(
        geocoding_base_url=settings["open_meteo"]["geocoding_base_url"]
    )

    fieldnames = [
        "city_id",
        "city_name",
        "country_code",
        "latitude",
        "longitude",
        "timezone",
        "admin1",
        "admin2",
        "population",
        "raw_geocoding_file",
    ]

    with open(dim_city_csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for city in cities_cfg["cities"]:
            city_id = city["city_id"]
            name = city["name"]
            country_code = city.get("country_code")

            result = client.geocode_city(name=name, country_code=country_code)
            if result is None:
                logger.error(
                    "Skipping city_id=%s (%s): no geocoding result",
                    city_id,
                    name,
                )
                continue

            raw_filename = f"{city_id}_{name.replace(' ', '_').lower()}.json"
            raw_path = geocoding_dir / raw_filename
            with open(raw_path, "w", encoding="utf-8") as jf:
                json.dump(result, jf, ensure_ascii=False, indent=2)

            row = {
                "city_id": city_id,
                "city_name": result.get("name", name),
                "country_code": result.get("country_code", country_code),
                "latitude": result.get("latitude"),
                "longitude": result.get("longitude"),
                "timezone": result.get("timezone"),
                "admin1": result.get("admin1"),
                "admin2": result.get("admin2"),
                "population": result.get("population"),
                "raw_geocoding_file": str(raw_path),
            }

            writer.writerow(row)
            logger.info(
                "Wrote dim_city row for city_id=%s (%s)",
                city_id,
                row["city_name"],
            )


if __name__ == "__main__":
    main()