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
    from constants import INDEX_HOSPITAL_DRUGS as INDEX_NAME
except ImportError:
    INDEX_NAME = "hospital-drugs"

TARGET_DRUG: str = "Insulin"
TARGET_WING: str = "Wing C"


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


def generate_docs(days: int = 180) -> List[Dict[str, object]]:
    """Generate synthetic hospital drug order/use records with waste.

    Args:
        days (int): Number of days of data to generate.

    Returns:
        list[dict]: Bulk API documents for Elasticsearch.
    """
    docs: List[Dict[str, object]] = []
    wings = ["Wing A", "Wing B", "Wing C", "Wing D"]
    for day in range(days):
        ts = datetime.utcnow() - timedelta(days=day)
        for wing in wings:
            base_orders = np.random.randint(20, 80)
            if wing == TARGET_WING:
                qty_ordered = int(base_orders * 1.4)
                qty_used = int(qty_ordered * np.random.uniform(0.5, 0.7))
            else:
                qty_ordered = base_orders
                qty_used = int(qty_ordered * np.random.uniform(0.85, 0.98))
            docs.append(
                {
                    "_index": INDEX_NAME,
                    "_source": {
                        "@timestamp": ts.isoformat() + "Z",
                        "drug_name": TARGET_DRUG,
                        "wing_id": wing,
                        "qty_ordered": max(0, qty_ordered),
                        "qty_used": max(0, qty_used),
                        "unit_cost_usd": 212.50,
                    },
                }
            )
    return docs


def main() -> None:
    """Generate and index synthetic hospital drug data."""
    try:
        es = get_client()
        docs = generate_docs()
        bulk(es, docs)
        logger.info("Indexed %d hospital drug records into %s", len(docs), INDEX_NAME)
    except ApiError as exc:
        logger.error("Failed to index hospital data", exc_info=exc)
        raise


if __name__ == "__main__":
    main()

