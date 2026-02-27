"""Bulk index all datasets: factory, hospital, NYC buildings, pricing reference."""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Project root for constants and data modules
_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_root))

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError
from elasticsearch.helpers import bulk

from constants import INDEX_PRICING_REFERENCE

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ghost-economy-setup")


def get_client() -> Elasticsearch:
    """Create Elasticsearch client from environment.

    Returns:
        Elasticsearch: Configured client.
    """
    url = os.getenv("ELASTIC_URL")
    api_key = os.getenv("ELASTIC_API_KEY")
    if not url or not api_key:
        raise ValueError("ELASTIC_URL and ELASTIC_API_KEY must be set in .env")
    return Elasticsearch(url, api_key=api_key)


def index_pricing_reference(es: Elasticsearch) -> None:
    """Index pricing reference documents from JSON or inline data.

    Args:
        es: Elasticsearch client.
    """
    docs = [
        {"_index": INDEX_PRICING_REFERENCE, "_id": "insulin", "_source": {"item_key": "insulin", "item_name": "Insulin (per unit)", "unit_cost_usd": 212.50, "unit_label": "units"}},
        {"_index": INDEX_PRICING_REFERENCE, "_id": "press-machine-hour", "_source": {"item_key": "press-machine-hour", "item_name": "Press Machine Idle Hour", "unit_cost_usd": 112.50, "unit_label": "hours"}},
        {"_index": INDEX_PRICING_REFERENCE, "_id": "kwh-nyc", "_source": {"item_key": "kwh-nyc", "item_name": "NYC Commercial Electricity", "unit_cost_usd": 0.19, "unit_label": "kwh"}},
    ]
    bulk(es, docs)
    logger.info("Indexed %d pricing reference documents", len(docs))


def main() -> None:
    """Run all data generators then index pricing reference."""
    try:
        es = get_client()
        from data.generate_factory import generate_docs as factory_docs
        from data.generate_hospital import generate_docs as hospital_docs
        from data.fetch_nyc_buildings import generate_docs as buildings_docs

        fdocs = factory_docs()
        bulk(es, fdocs)
        logger.info("Indexed %d factory IoT records", len(fdocs))

        hdocs = hospital_docs()
        bulk(es, hdocs)
        logger.info("Indexed %d hospital drug records", len(hdocs))

        bdocs = buildings_docs()
        bulk(es, bdocs)
        logger.info("Indexed %d NYC building records", len(bdocs))

        index_pricing_reference(es)
    except ApiError as exc:
        logger.error("Indexing failed", exc_info=exc)
        raise


if __name__ == "__main__":
    main()
