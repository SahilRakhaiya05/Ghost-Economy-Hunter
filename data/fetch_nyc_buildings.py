"""Generate and index synthetic NYC building energy/occupancy data for energy_anomaly detection."""
from __future__ import annotations

from datetime import datetime, timedelta
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError
from elasticsearch.helpers import bulk

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ghost-economy-data")

# Project root for constants
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from constants import INDEX_NYC_BUILDINGS
except ImportError:
    INDEX_NYC_BUILDINGS = "nyc-buildings"

ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]


def get_client() -> Elasticsearch:
    """Create and return an Elasticsearch client.

    Returns:
        Elasticsearch: Configured client.
    """
    if not ELASTIC_URL or not ELASTIC_API_KEY:
        raise ValueError("ELASTIC_URL and ELASTIC_API_KEY must be set in the environment.")
    return Elasticsearch(ELASTIC_URL, api_key=ELASTIC_API_KEY)


def generate_docs(days: int = 90, buildings_per_borough: int = 4) -> List[Dict[str, object]]:
    """Generate synthetic NYC building energy and occupancy records.

    Args:
        days: Number of days of data.
        buildings_per_borough: Buildings per borough.

    Returns:
        list[dict]: Bulk API documents for Elasticsearch.
    """
    docs: List[Dict[str, object]] = []
    for day in range(days):
        ts = datetime.utcnow() - timedelta(days=day)
        for bi, borough in enumerate(BOROUGHS):
            for b in range(buildings_per_borough):
                building_id = f"BLD-{borough[:2].upper()}-{b:02d}"
                sqft = int(np.random.uniform(5000, 50000))
                # Some buildings with low occupancy but high energy (anomaly candidates)
                low_occupancy = bi == 0 and b == 0
                occupancy_pct = float(np.random.uniform(0.03, 0.12) if low_occupancy else np.random.uniform(0.2, 0.9))
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
