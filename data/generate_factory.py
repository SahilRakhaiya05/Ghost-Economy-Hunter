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

# Project root for constants
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ghost-economy-data")

ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")

try:
    from constants import INDEX_FACTORY_IOT as INDEX_NAME
except ImportError:
    INDEX_NAME = "factory-iot-data"

MACHINES: List[str] = ["PRESS-01", "PRESS-02", "PRESS-03"]
ANOMALY_MACHINE: str = "PRESS-02"


def get_client() -> Elasticsearch:
    """Create and return an Elasticsearch client.

    Args:
        None

    Returns:
        Elasticsearch: Configured Elasticsearch client.
    """
    if not ELASTIC_URL or not ELASTIC_API_KEY:
        raise ValueError("ELASTIC_URL and ELASTIC_API_KEY must be set in the environment.")
    return Elasticsearch(ELASTIC_URL, api_key=ELASTIC_API_KEY)


def generate_docs(days: int = 90) -> List[Dict[str, object]]:
    """Generate synthetic factory IoT records with anomalies.

    Args:
        days (int): Number of days of data to generate.

    Returns:
        list[dict]: Bulk API documents for Elasticsearch.
    """
    docs: List[Dict[str, object]] = []
    for day in range(days):
        ts = datetime.utcnow() - timedelta(days=day)
        hour = day % 24
        shift_active = 8 <= hour <= 16
        for machine in MACHINES:
            if shift_active:
                runtime = int(np.random.normal(460, 20))
                units = int(runtime * np.random.uniform(0.75, 0.85))
            else:
                if machine == ANOMALY_MACHINE:
                    runtime = int(np.random.normal(380, 30))
                else:
                    runtime = int(np.random.normal(45, 15))
                units = 0
            docs.append(
                {
                    "_index": INDEX_NAME,
                    "_source": {
                        "@timestamp": ts.isoformat() + "Z",
                        "machine_id": machine,
                        "shift_active": shift_active,
                        "runtime_minutes": max(0, runtime),
                        "production_units": max(0, units),
                        "cost_per_hour": 112.50,
                    },
                }
            )
    return docs


def main() -> None:
    """Generate and index synthetic factory IoT data."""
    try:
        es = get_client()
        docs = generate_docs()
        bulk(es, docs)
        logger.info("Indexed %d factory IoT records into %s", len(docs), INDEX_NAME)
    except ApiError as exc:
        logger.error("Failed to index factory data", exc_info=exc)
        raise


if __name__ == "__main__":
    main()

