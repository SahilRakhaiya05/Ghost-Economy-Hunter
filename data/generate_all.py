"""Ghost Economy Hunter — Convenience data generator.

Generates and indexes all synthetic data for all three domains:
  - Factory IoT (90 days, 3 machines)
  - Hospital drugs (180 days, 4 wings, 3 drugs)
  - NYC buildings (365 days, 3 buildings)
  - Pricing reference (5 items)

Usage:
    python data/generate_all.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import ApiError

from data.generate_factory import generate_docs as factory_docs
from data.generate_hospital import generate_docs as hospital_docs
from data.fetch_nyc_buildings import generate_docs as buildings_docs

import os

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ghost-economy-generate")

PRICING_ITEMS = [
    {"item_key": "insulin",             "item_name": "Insulin per unit",            "unit_cost_usd": 212.50, "unit_label": "units"},
    {"item_key": "metformin",           "item_name": "Metformin per unit",           "unit_cost_usd": 45.00,  "unit_label": "units"},
    {"item_key": "amoxicillin",         "item_name": "Amoxicillin per unit",         "unit_cost_usd": 18.00,  "unit_label": "units"},
    {"item_key": "press-machine-hour",  "item_name": "Press Machine Idle Hour",      "unit_cost_usd": 112.50, "unit_label": "hours"},
    {"item_key": "kwh-nyc",             "item_name": "NYC Commercial Electricity",   "unit_cost_usd": 0.19,   "unit_label": "kwh"},
]


def main() -> None:
    """Generate and index all synthetic data.

    Returns:
        None
    """
    es = Elasticsearch(
        os.getenv("ELASTIC_URL", ""),
        api_key=os.getenv("ELASTIC_API_KEY", ""),
    )

    try:
        log.info("Generating factory IoT data...")
        fdocs = factory_docs()
        bulk(es, fdocs)
        log.info("Indexed %d factory records", len(fdocs))

        log.info("Generating hospital drug data...")
        hdocs = hospital_docs()
        bulk(es, hdocs)
        log.info("Indexed %d hospital records", len(hdocs))

        log.info("Generating NYC building data...")
        bdocs = buildings_docs()
        bulk(es, bdocs)
        log.info("Indexed %d building records", len(bdocs))

        log.info("Indexing pricing reference...")
        for item in PRICING_ITEMS:
            es.index(index="pricing-reference", id=item["item_key"], document=item)
        log.info("Indexed %d pricing items", len(PRICING_ITEMS))

        log.info("ALL DATA READY")

    except ApiError as exc:
        log.error("Data generation failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
