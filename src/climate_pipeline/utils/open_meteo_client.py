import logging
from typing import Optional, Dict, Any, List

import requests

import time
from requests import HTTPError

logger = logging.getLogger(__name__)


class OpenMeteoClient:
    def __init__(self, geocoding_base_url: str, historical_base_url: str):
        self.geocoding_base_url = geocoding_base_url
        self.historical_base_url = historical_base_url

    def geocode_city(
        self,
        name: str,
        country_code: Optional[str] = None,
        count: int = 1,
        language: str = "en",
        timeout: int = 10,
    ) -> Optional[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "name": name,
            "count": count,
            "language": language,
        }
        if country_code:
            params["country_code"] = country_code

        logger.info("Geocoding city: %s (%s)", name, country_code or "no country filter")
        resp = requests.get(self.geocoding_base_url, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        results: List[Dict[str, Any]] = data.get("results") or []
        if not results:
            logger.warning("No geocoding results for %s (%s)", name, country_code)
            return None

        best = results[0]
        logger.info(
            "Geocoding result for %s: name=%s, lat=%.4f, lon=%.4f, tz=%s",
            name,
            best.get("name"),
            best.get("latitude"),
            best.get("longitude"),
            best.get("timezone"),
        )
        return best

    def fetch_daily_history(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        daily_variables: List[str],
        timezone: str = "auto",
        timeout: int = 30,
        max_retries: int = 5,
        backoff_seconds: float = 2.0,
    ) -> Dict[str, Any]:
        """
        Fetch daily historical weather data for a given coordinate and date range.

        Retries on HTTP 429 (Too Many Requests) with exponential backoff.
        Dates are strings in YYYY-MM-DD format. Returns parsed JSON.
        """
        params: Dict[str, Any] = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(daily_variables),
            "timezone": timezone,
        }

        attempt = 0
        while True:
            attempt += 1
            logger.info(
                "Fetching daily history lat=%.4f lon=%.4f %s → %s (attempt %d)",
                latitude,
                longitude,
                start_date,
                end_date,
                attempt,
            )
            try:
                resp = requests.get(self.historical_base_url, params=params, timeout=timeout)
                resp.raise_for_status()
                data = resp.json()

                if "daily" not in data:
                    logger.warning(
                        "No 'daily' key in historical response for %s → %s",
                        start_date,
                        end_date,
                    )
                return data

            except HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                # Handle rate limiting with retry
                if status == 429 and attempt <= max_retries:
                    sleep_for = backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "Received 429 Too Many Requests. Backing off for %.1f seconds (attempt %d/%d).",
                        sleep_for,
                        attempt,
                        max_retries,
                    )
                    time.sleep(sleep_for)
                    continue

                logger.error(
                    "HTTP error when fetching daily history (%s → %s): %s",
                    start_date,
                    end_date,
                    e,
                )
                raise

            except Exception as e:
                logger.error(
                    "Unexpected error when fetching daily history (%s → %s): %s",
                    start_date,
                    end_date,
                    e,
                )
                raise