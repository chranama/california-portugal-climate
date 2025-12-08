import argparse
import json
import logging
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple
import time

import yaml

from climate_pipeline.utils.open_meteo_client import OpenMeteoClient
from climate_pipeline.utils.load_yaml_with_env import load_yaml_with_env


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


def clamp_year_window(start_date: date, end_date: date, year: int) -> Tuple[date, date]:
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    start = max(year_start, start_date)
    end = min(year_end, end_date)
    return start, end


def slugify_city_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


# ============================
# CLI helpers
# ============================

def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date format (expected YYYY-MM-DD): {value}"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch daily weather data from Open-Meteo"
    )

    parser.add_argument(
        "--mode",
        choices=["backfill", "recent"],
        default="recent",
        help=(
            "Ingestion mode. "
            "'backfill' = use a specified historical window "
            "(defaults to settings.yaml if no dates passed); "
            "'recent' = fetch only missing recent days based on existing data "
            "(date range will be computed, ignoring settings.yaml time_window). "
            "Default: recent."
        ),
    )

    parser.add_argument(
        "--start-date",
        type=parse_iso_date,
        help="Start date for backfill (YYYY-MM-DD). If omitted, uses settings.yaml time_window.start_date.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_iso_date,
        help="End date for backfill (YYYY-MM-DD). If omitted, uses settings.yaml time_window.end_date.",
    )

    # Optional: allow overriding the settings path
    parser.add_argument(
        "--settings-path",
        default="src/config/settings.yaml",
        help="Path to settings YAML file (default: src/config/settings.yaml).",
    )

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.mode == "backfill":
        # It’s OK if start/end are None here; we'll fall back to settings.yaml.
        if args.start_date and args.end_date and args.start_date > args.end_date:
            raise SystemExit(
                f"Error: start_date ({args.start_date}) cannot be after end_date ({args.end_date})."
            )
    elif args.mode == "recent":
        # Dates are not used in recent mode; warn if user passed them.
        if args.start_date is not None or args.end_date is not None:
            logging.warning(
                "--start-date/--end-date are ignored in recent mode. "
                "Use --mode backfill if you want a fixed historical window."
            )


# ============================
# Core ingestion modes
# ============================

def run_backfill(
    settings: Dict[str, Any],
    cities: List[City],
    start_date: date,
    end_date: date,
    logger: logging.Logger,
) -> None:
    raw_weather_dir = Path(settings["data"]["raw_weather_dir"])
    raw_weather_dir.mkdir(parents=True, exist_ok=True)

    daily_vars: List[str] = settings["open_meteo"]["daily_variables"]

    client = OpenMeteoClient(
        geocoding_base_url=settings["open_meteo"]["geocoding_base_url"],
        historical_base_url=settings["open_meteo"]["historical_base_url"],
    )

    years = year_range(start_date, end_date)
    logger.info("Backfill mode: fetching daily data for years: %s", years)

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
            year_start, year_end = clamp_year_window(start_date, end_date, year)
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


def run_recent(
    settings: Dict[str, Any],
    cities: List[City],
    logger: logging.Logger,
) -> None:
    """
    Incremental/recent mode.

    Strategy:
    - For each city, fetch year-to-date daily weather for the current year.
    - Overwrite the existing <year>.json file (if any) for that city.
    - Historical years remain untouched (they are populated via backfill).

    This keeps the raw landing zone structure:
        <raw_weather_dir>/<city_slug>/<year>.json
    but ensures the current year's file always reflects the latest available data.
    """

    raw_weather_dir = Path(settings["data"]["raw_weather_dir"])
    raw_weather_dir.mkdir(parents=True, exist_ok=True)

    daily_vars: List[str] = settings["open_meteo"]["daily_variables"]

    client = OpenMeteoClient(
        geocoding_base_url=settings["open_meteo"]["geocoding_base_url"],
        historical_base_url=settings["open_meteo"]["historical_base_url"],
    )

    today = date.today()
    current_year = today.year

    # Decide whether to include today or stop at yesterday.
    # Using yesterday is often saner for a "completed-day" view:
    end_date = today - timedelta(days=1)
    year_start = date(current_year, 1, 1)

    if end_date < year_start:
        logger.warning(
            "Recent mode: end_date (%s) is before the start of the current year (%s). "
            "Nothing to fetch.",
            end_date,
            year_start,
        )
        return

    logger.info(
        "Recent mode: fetching year-to-date data for current year %s (%s → %s)",
        current_year,
        year_start,
        end_date,
    )

    for city in cities:
        city_slug = slugify_city_name(city.city_name)
        city_dir = raw_weather_dir / city_slug
        city_dir.mkdir(parents=True, exist_ok=True)

        out_path = city_dir / f"{current_year}.json"

        logger.info(
            "Recent mode: requesting daily history for city_id=%s name=%s year=%s (%s → %s)",
            city.city_id,
            city.city_name,
            current_year,
            year_start,
            end_date,
        )

        data = client.fetch_daily_history(
            latitude=city.latitude,
            longitude=city.longitude,
            start_date=year_start.isoformat(),
            end_date=end_date.isoformat(),
            daily_variables=daily_vars,
            timezone=city.timezone or "auto",
        )

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info("Recent mode: wrote %s", out_path)

        # Be nice to the API: small pause between requests
        time.sleep(0.5)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args)

    # Load settings from YAML (path may be overridden by CLI)
    settings = load_yaml_with_env(args.settings_path)

    # Logging setup
    log_path = settings["logging"]["ingestion_log"]
    setup_logging(log_path)
    logger = logging.getLogger("fetch_daily_weather")

    # Load cities
    dim_city_path = Path("dbt/seeds/dim_city.csv")
    if not dim_city_path.exists():
        logger.error("dim_city.csv not found at %s. Run geocode-cities first.", dim_city_path)
        return

    cities = load_cities_from_dim_city(str(dim_city_path))
    logger.info("Loaded %d cities from dim_city.csv", len(cities))

    if args.mode == "backfill":
        # Use CLI dates if given, otherwise fall back to settings.yaml
        if args.start_date is not None:
            start_date = args.start_date
        else:
            start_date = datetime.strptime(
                settings["time_window"]["start_date"], "%Y-%m-%d"
            ).date()

        if args.end_date is not None:
            end_date = args.end_date
        else:
            end_date = datetime.strptime(
                settings["time_window"]["end_date"], "%Y-%m-%d"
            ).date()

        run_backfill(settings=settings, cities=cities, start_date=start_date, end_date=end_date, logger=logger)

    elif args.mode == "recent":
        run_recent(settings=settings, cities=cities, logger=logger)
    else:
        raise SystemExit(f"Unknown mode: {args.mode}")


if __name__ == "__main__":
    main()