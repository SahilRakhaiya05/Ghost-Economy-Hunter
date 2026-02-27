"""Elasticsearch connection helper. All ES access should use this or agent tools."""
from __future__ import annotations

import logging
import os
from typing import Optional

from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()

logger = logging.getLogger("ghost-economy")


def get_elastic_client() -> Elasticsearch:
    """Create and return an Elasticsearch client from environment variables.

    Returns:
        Elasticsearch: Configured client.

    Raises:
        ValueError: If ELASTIC_URL or ELASTIC_API_KEY are missing.
    """
    url: Optional[str] = os.getenv("ELASTIC_URL")
    api_key: Optional[str] = os.getenv("ELASTIC_API_KEY")
    if not url or not api_key:
        raise ValueError("ELASTIC_URL and ELASTIC_API_KEY must be set in .env")
    return Elasticsearch(url, api_key=api_key)
