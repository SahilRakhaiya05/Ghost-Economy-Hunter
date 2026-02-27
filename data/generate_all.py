"""Ghost Economy Hunter -- Convenience data generator.

Generates and indexes all data for all three domains:
  - Factory IoT (90 days, 3 machines)
  - Hospital drugs (180 days, 4 wings)
  - NYC buildings (real NYC Open Data + synthetic anomaly overlay)
  - Pricing reference (5 items with real market rates)

Usage:
    python data/generate_all.py
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError
from elasticsearch.helpers import bulk

from data.generate_factory import generate_docs as factory_docs
from data.generate_hospital import generate_docs as hospital_docs
from data.fetch_nyc_buildings import generate_docs as buildings_docs

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ghost-economy-generate")


def _load_pricing() -> list:
    """Load pricing items from pricing_reference.json.

    Returns:
        List of pricing item dicts.
    """
    pricing_path = Path(__file__).parent / "pricing_reference.json"
    with open(pricing_path, encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    """Generate and index all data into Elasticsearch.

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

        log.info("Generating NYC building data (fetching from NYC Open Data)...")
        bdocs = buildings_docs()
        bulk(es, bdocs)
        log.info("Indexed %d building records", len(bdocs))

        log.info("Indexing pricing reference (real market rates)...")
        pricing_items = _load_pricing()
        for item in pricing_items:
            es.index(index="pricing-reference", id=item["item_key"], document=item)
        log.info("Indexed %d pricing items", len(pricing_items))

        log.info("Indexing known-exceptions (sample records)...")
        exceptions = [
            {
                "entity_id": "PRESS-01",
                "reason": "Scheduled maintenance window - approved by operations manager",
                "start_date": "2025-12-01",
                "end_date": "2026-01-15",
            },
            {
                "entity_id": "Wing A",
                "reason": "Renovation period - temporary over-ordering approved by pharmacy board",
                "start_date": "2025-11-01",
                "end_date": "2026-03-01",
            },
        ]
        for exc_record in exceptions:
            es.index(
                index="known-exceptions",
                id=exc_record["entity_id"],
                document=exc_record,
            )
        log.info("Indexed %d known-exception records", len(exceptions))

        log.info("ALL DATA READY")

    except ApiError as exc:
        log.error("Data generation failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
