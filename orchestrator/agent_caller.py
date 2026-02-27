"""Agent Builder API wrapper. All agent calls must go through this module."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("ghost-economy")


def call_agent(agent_id: str, message: str) -> Dict[str, Any]:
    """Call an Elastic Agent Builder agent via the Kibana API.

    Args:
        agent_id: The agent ID from Kibana Agent Builder.
        message: User message content to send.

    Returns:
        Parsed JSON response from the agent (must be valid JSON per project rules).

    Raises:
        ValueError: If KIBANA_URL or ELASTIC_API_KEY are missing.
        requests.HTTPError: On non-2xx response.
    """
    base_url = os.getenv("KIBANA_URL")
    api_key = os.getenv("ELASTIC_API_KEY")
    if not base_url or not api_key:
        raise ValueError("KIBANA_URL and ELASTIC_API_KEY must be set in .env")

    url = f"{base_url.rstrip('/')}/api/agent_builder/agents/{agent_id}/chat"
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
    }
    payload: Dict[str, Any] = {
        "messages": [{"role": "user", "content": message}],
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Agent API request failed for agent_id=%s: %s", agent_id, exc)
        raise

    raw = resp.json()
    content = raw.get("message", {}).get("content", "{}")
    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        logger.error("Agent response was not valid JSON: %s", exc)
        raise


class AgentCaller:
    """Wrapper for Agent Builder API. Use call_agent() for simple usage."""

    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def chat(self, agent_id: str, message: str) -> Dict[str, Any]:
        """Send a chat message to an agent and return parsed JSON."""
        return call_agent(agent_id, message)
