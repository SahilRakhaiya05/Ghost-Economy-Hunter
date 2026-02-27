"""Create all Elasticsearch indexes with explicit mappings. Run once before indexing data."""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from elasticsearch.exceptions import NotFoundError

# Project root for constants
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from constants import (
    INDEX_FACTORY_IOT,
    INDEX_GHOST_ECONOMY_AUDIT,
    INDEX_HOSPITAL_DRUGS,
    INDEX_KNOWN_EXCEPTIONS,
    INDEX_NYC_BUILDINGS,
    INDEX_PRICING_REFERENCE,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ghost-economy-setup")

INDEXES: Dict[str, Dict[str, Any]] = {
    INDEX_FACTORY_IOT: {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "machine_id": {"type": "keyword"},
                "shift_active": {"type": "boolean"},
                "runtime_minutes": {"type": "integer"},
                "production_units": {"type": "integer"},
                "cost_per_hour": {"type": "float"},
            }
        }
    },
    INDEX_HOSPITAL_DRUGS: {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "drug_name": {"type": "keyword"},
                "wing_id": {"type": "keyword"},
                "qty_ordered": {"type": "integer"},
                "qty_used": {"type": "integer"},
                "unit_cost_usd": {"type": "float"},
            }
        }
    },
    INDEX_NYC_BUILDINGS: {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "building_id": {"type": "keyword"},
                "borough": {"type": "keyword"},
                "occupancy_pct": {"type": "float"},
                "energy_kwh": {"type": "float"},
                "sqft": {"type": "integer"},
            }
        }
    },
    INDEX_PRICING_REFERENCE: {
        "mappings": {
            "properties": {
                "item_key": {"type": "keyword"},
                "item_name": {"type": "text"},
                "unit_cost_usd": {"type": "float"},
                "unit_label": {"type": "keyword"},
            }
        }
    },
    INDEX_KNOWN_EXCEPTIONS: {
        "mappings": {
            "properties": {
                "entity_id": {"type": "keyword"},
                "reason": {"type": "text"},
                "start_date": {"type": "date"},
                "end_date": {"type": "date"},
            }
        }
    },
    INDEX_GHOST_ECONOMY_AUDIT: {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "finding_id": {"type": "keyword"},
                "entity": {"type": "text"},
                "dollar_value": {"type": "double"},
                "action_taken": {"type": "keyword"},
                "full_json": {"type": "object", "enabled": False},
            }
        }
    },
}


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


def create_index(es: Elasticsearch, name: str, body: Dict[str, Any]) -> None:
    """Create one index if it does not exist.

    Args:
        es: Elasticsearch client.
        name: Index name.
        body: Index body (mappings, etc.).
    """
    try:
        if not es.indices.exists(index=name):
            es.indices.create(index=name, body=body)
            logger.info("Created index: %s", name)
        else:
            logger.info("Index already exists: %s", name)
    except ESConnectionError as exc:
        logger.error("Connection failed for index %s", name, exc_info=exc)
        raise
    except Exception as exc:
        logger.error("Failed to create index %s", name, exc_info=exc)
        raise


def main() -> None:
    """Create all project indexes with explicit mappings."""
    try:
        es = get_client()
        for name, body in INDEXES.items():
            create_index(es, name, body)
    except (ValueError, ESConnectionError, NotFoundError) as exc:
        logger.error("Setup failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
