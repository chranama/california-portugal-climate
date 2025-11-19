import json
import logging
import csv
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List
import time

import yaml

from climate_pipeline.utils.open_meteo_client import OpenMeteoClient


@dataclass
class City:
    city_id: int
    city_name: str
    country_code: str
    latitude: float
    longitude: float
    timezone: str


def setup_logging(log_path: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(console)


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_cities_from_dim_city(path: str) -> List[City]:
    cities: List[City] = []
    with open(path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cities.append(
                City(
                    city_id=int(row["city_id"]),
                    city_name=row["city_name"],
                    country_code=row["country_code"],
                    latitude=float(row["latitude"]),
                    longitude=float(row["longitude"]),
                    timezone=row["timezone"],
                )
            )
    return cities


def year_range(start_date: date, end_date: date) -> List[int]:
    return list(range(start_date.year, end_date.year + 1))


def clamp_year_window(year: int, start_date: date, end_date: date) -> (date, date):
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    start = max(year_start, start_date)
    end = min(year_end, end_date)
    return start, end


def slugify_city_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def main() -> None:
    settings = load_yaml("src/config/settings.yaml")
    log_path = settings["logging"]["ingestion_log"]
    setup_logging(log_path)
    logger = logging.getLogger("fetch_daily_weather")

    # Paths
    raw_weather_dir = Path(settings["data"]["raw_weather_dir"])
    raw_weather_dir.mkdir(parents=True, exist_ok=True)

    dim_city_path = Path("dbt/seeds/dim_city.csv")
    if not dim_city_path.exists():
        logger.error("dim_city.csv not found at %s. Run geocode-cities first.", dim_city_path)
        return

    cities = load_cities_from_dim_city(str(dim_city_path))
    logger.info("Loaded %d cities from dim_city.csv", len(cities))

    # Time window
    start_date_str = settings["time_window"]["start_date"]
    end_date_str = settings["time_window"]["end_date"]
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    years = year_range(start_date, end_date)
    logger.info("Fetching daily data for years: %s", years)

    daily_vars: List[str] = settings["open_meteo"]["daily_variables"]

    client = OpenMeteoClient(
        geocoding_base_url=settings["open_meteo"]["geocoding_base_url"],
        historical_base_url=settings["open_meteo"]["historical_base_url"],
    )

    for city in cities:
        city_slug = slugify_city_name(city.city_name)
        city_dir = raw_weather_dir / city_slug
        city_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Starting downloads for city_id=%s name=%s",
            city.city_id,
            city.city_name,
        )

        for year in years:
            year_start, year_end = clamp_year_window(year, start_date, end_date)
            if year_start > year_end:
                continue  # outside global window

            out_path = city_dir / f"{year}.json"
            if out_path.exists():
                logger.info(
                    "Skipping existing file for %s %s: %s",
                    city.city_name,
                    year,
                    out_path,
                )
                continue

            logger.info(
                "Requesting daily history for %s year=%s (%s → %s)",
                city.city_name,
                year,
                year_start,
                year_end,
            )

            data = client.fetch_daily_history(
                latitude=city.latitude,
                longitude=city.longitude,
                start_date=year_start.isoformat(),
                end_date=year_end.isoformat(),
                daily_variables=daily_vars,
                timezone=city.timezone or "auto",
            )

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info("Wrote %s", out_path)

            # Be nice to the API: small pause between requests
            time.sleep(0.5)


if __name__ == "__main__":
    main()