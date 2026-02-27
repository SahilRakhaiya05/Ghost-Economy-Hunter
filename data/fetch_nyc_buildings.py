"""Fetch real NYC building energy data from NYC Open Data (LL84) and generate ES documents.

Uses the NYC Building Energy and Water Data Disclosure dataset (Local Law 84):
https://data.cityofnewyork.us/resource/5zyy-y8am.json

Real building data is fetched, then one synthetic anomaly building is injected
(very low occupancy + very high energy) so the pipeline always detects waste.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import numpy as np
import requests as http_client
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError
from elasticsearch.helpers import bulk

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ghost-economy-data")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from constants import INDEX_NYC_BUILDINGS
except ImportError:
    INDEX_NYC_BUILDINGS = "nyc-buildings"

ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")

NYC_OPEN_DATA_URL = "https://data.cityofnewyork.us/resource/5zyy-y8am.json"

BOROUGH_MAP = {
    "1": "Manhattan",
    "2": "Bronx",
    "3": "Brooklyn",
    "4": "Queens",
    "5": "Staten Island",
}


def get_client() -> Elasticsearch:
    """Create and return an Elasticsearch client.

    Returns:
        Elasticsearch: Configured client.
    """
    if not ELASTIC_URL or not ELASTIC_API_KEY:
        raise ValueError("ELASTIC_URL and ELASTIC_API_KEY must be set in the environment.")
    return Elasticsearch(ELASTIC_URL, api_key=ELASTIC_API_KEY)


def _fetch_real_buildings(limit: int = 200) -> List[Dict]:
    """Fetch real building energy records from NYC Open Data LL84 API.

    Args:
        limit: Max number of buildings to fetch.

    Returns:
        List of raw JSON records from the API.
    """
    try:
        resp = http_client.get(NYC_OPEN_DATA_URL, timeout=20)
        if resp.status_code != 200:
            logger.warning("NYC Open Data returned %d — falling back to synthetic", resp.status_code)
            return []
        data = resp.json()
        logger.info("Fetched %d real building records from NYC Open Data", len(data))
        return data[:limit]
    except http_client.RequestException as exc:
        logger.warning("NYC Open Data fetch failed (%s) — falling back to synthetic", exc)
        return []


def _parse_real_building(raw: Dict) -> Dict | None:
    """Parse a raw NYC Open Data record into our index schema.

    Args:
        raw: Single JSON record from the API.

    Returns:
        Parsed dict matching nyc-buildings schema, or None if unparseable.
    """
    try:
        bbl = raw.get("nyc_borough_block_and_lot", "")
        borough_code = bbl[0] if bbl else ""
        borough = BOROUGH_MAP.get(borough_code, "Unknown")

        prop_name = raw.get("property_name", "Unknown")
        building_id = f"NYC-{bbl[:10]}" if bbl else f"NYC-{prop_name[:8]}"

        occ_raw = raw.get("occupancy", "50")
        occupancy_pct = float(occ_raw) / 100.0 if occ_raw and occ_raw != "Not Available" else 0.5

        kwh_raw = raw.get("electricity_use_grid_purchase_1", "0")
        energy_kwh = float(kwh_raw) if kwh_raw and kwh_raw != "Not Available" else 0

        sqft_raw = raw.get("largest_property_use_type_1", "0")
        sqft = int(float(sqft_raw)) if sqft_raw and sqft_raw != "Not Available" else 10000

        if energy_kwh <= 0 or sqft <= 0:
            return None

        return {
            "building_id": building_id,
            "borough": borough,
            "occupancy_pct": round(max(0.01, min(1.0, occupancy_pct)), 4),
            "energy_kwh": round(energy_kwh, 2),
            "sqft": sqft,
        }
    except (ValueError, TypeError, IndexError):
        return None


def generate_docs(days: int = 90, buildings_per_borough: int = 4) -> List[Dict[str, object]]:
    """Generate NYC building energy documents from real data + synthetic anomaly overlay.

    Fetches real building data from NYC Open Data (LL84 disclosure).
    Injects one synthetic anomaly building (BLD-MA-00: low occupancy, high energy)
    so the pipeline always finds at least one energy-occupancy divergence.

    Args:
        days: Number of days of time-series data to generate.
        buildings_per_borough: Fallback buildings per borough if API is unavailable.

    Returns:
        list[dict]: Bulk API documents for Elasticsearch.
    """
    docs: List[Dict[str, object]] = []

    real_records = _fetch_real_buildings(limit=150)
    parsed_buildings = []
    for rec in real_records:
        parsed = _parse_real_building(rec)
        if parsed:
            parsed_buildings.append(parsed)

    if len(parsed_buildings) < 5:
        logger.info("Insufficient real data (%d records) — using synthetic generation", len(parsed_buildings))
        return _generate_synthetic_docs(days, buildings_per_borough)

    logger.info("Using %d real buildings from NYC Open Data", len(parsed_buildings))

    anomaly_building = {
        "building_id": "BLD-MA-00",
        "borough": "Manhattan",
        "occupancy_pct": None,
        "energy_kwh": None,
        "sqft": 40000,
        "is_anomaly": True,
    }

    for day in range(days):
        ts = datetime.utcnow() - timedelta(days=day)

        for bldg in parsed_buildings[:20]:
            occ_jitter = np.random.uniform(-0.05, 0.05)
            occ = max(0.01, min(1.0, bldg["occupancy_pct"] + occ_jitter))
            kwh_jitter = bldg["energy_kwh"] * np.random.uniform(-0.15, 0.15)
            kwh = max(1.0, bldg["energy_kwh"] + kwh_jitter)

            docs.append({
                "_index": INDEX_NYC_BUILDINGS,
                "_source": {
                    "@timestamp": ts.isoformat() + "Z",
                    "building_id": bldg["building_id"],
                    "borough": bldg["borough"],
                    "occupancy_pct": round(occ, 4),
                    "energy_kwh": round(kwh, 2),
                    "sqft": bldg["sqft"],
                },
            })

        occ_anomaly = float(np.random.uniform(0.03, 0.12))
        kwh_anomaly = float(np.random.uniform(820, 960))
        docs.append({
            "_index": INDEX_NYC_BUILDINGS,
            "_source": {
                "@timestamp": ts.isoformat() + "Z",
                "building_id": anomaly_building["building_id"],
                "borough": anomaly_building["borough"],
                "occupancy_pct": round(occ_anomaly, 4),
                "energy_kwh": round(kwh_anomaly, 2),
                "sqft": anomaly_building["sqft"],
            },
        })

    return docs


def _generate_synthetic_docs(days: int, buildings_per_borough: int) -> List[Dict[str, object]]:
    """Fallback: generate purely synthetic building data.

    Args:
        days: Number of days of data.
        buildings_per_borough: Buildings per borough.

    Returns:
        list[dict]: Bulk API documents for Elasticsearch.
    """
    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
    docs: List[Dict[str, object]] = []
    for day in range(days):
        ts = datetime.utcnow() - timedelta(days=day)
        for bi, borough in enumerate(boroughs):
            for b in range(buildings_per_borough):
                building_id = f"BLD-{borough[:2].upper()}-{b:02d}"
                sqft = int(np.random.uniform(5000, 50000))
                low_occupancy = bi == 0 and b == 0
                occupancy_pct = float(
                    np.random.uniform(0.03, 0.12) if low_occupancy else np.random.uniform(0.2, 0.9)
                )
                base_kwh = sqft * 0.02 * (1 + np.random.uniform(-0.2, 0.2))
                energy_kwh = base_kwh * (2.0 if low_occupancy else 1.0)
                docs.append({
                    "_index": INDEX_NYC_BUILDINGS,
                    "_source": {
                        "@timestamp": ts.isoformat() + "Z",
                        "building_id": building_id,
                        "borough": borough,
                        "occupancy_pct": round(occupancy_pct, 4),
                        "energy_kwh": round(energy_kwh, 2),
                        "sqft": sqft,
                    },
                })
    return docs


def main() -> None:
    """Generate and index NYC building data."""
    try:
        es = get_client()
        docs = generate_docs()
        bulk(es, docs)
        logger.info("Indexed %d NYC building records into %s", len(docs), INDEX_NYC_BUILDINGS)
    except ApiError as exc:
        logger.error("Failed to index NYC buildings data", exc_info=exc)
        raise


if __name__ == "__main__":
    main()
