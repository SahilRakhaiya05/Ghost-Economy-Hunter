"""Agent Builder API wrapper. All agent calls must go through this module.

Uses the correct Kibana Agent Builder REST API endpoints:
  - POST /api/agent_builder/converse       (synchronous chat)
  - POST /api/agent_builder/converse/async  (streaming SSE chat)
  - GET  /api/agent_builder/agents          (list agents)
  - GET  /api/agent_builder/tools           (list tools)
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("ghost-economy")

_MAX_RETRIES = 1
_RETRY_DELAY_S = 5
_TIMEOUT_S = 120


def _base_url() -> str:
    """Return the Kibana base URL from env."""
    return os.getenv("KIBANA_URL", "").rstrip("/")


def _api_key() -> str:
    """Return the Elastic API key from env."""
    return os.getenv("ELASTIC_API_KEY", "")


def _headers() -> Dict[str, str]:
    """Build auth headers for Agent Builder API.

    Returns:
        Dict of HTTP headers.
    """
    return {
        "Authorization": f"ApiKey {_api_key()}",
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
    }


def test_connection() -> Dict[str, Any]:
    """Test connectivity to the Agent Builder API.

    Returns:
        Dict with 'connected' bool, 'status_code', and optional 'error'.
    """
    base = _base_url()
    if not base or not _api_key():
        return {"connected": False, "error": "KIBANA_URL or ELASTIC_API_KEY not set"}
    try:
        resp = requests.get(
            f"{base}/api/agent_builder/agents",
            headers=_headers(),
            timeout=10,
        )
        if resp.status_code == 200:
            agents = resp.json()
            count = len(agents) if isinstance(agents, list) else len(agents.get("agents", []))
            return {"connected": True, "status_code": 200, "agent_count": count}
        return {"connected": False, "status_code": resp.status_code, "error": resp.text[:200]}
    except requests.ConnectionError:
        return {"connected": False, "error": "Connection refused — is Kibana running?"}
    except requests.Timeout:
        return {"connected": False, "error": "Connection timed out"}
    except requests.RequestException as exc:
        return {"connected": False, "error": str(exc)}


def _extract_list(data: Any, keys: tuple) -> List[Dict[str, Any]]:
    """Extract a list of items from various API response shapes.

    Args:
        data: Parsed JSON response (list or dict).
        keys: Possible dict keys to try (e.g. ("agents", "items", "data")).

    Returns:
        List of dicts.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in keys:
            if k in data and isinstance(data[k], list):
                return data[k]
    return []


def _load_local_agents() -> List[Dict[str, Any]]:
    """Load agent definitions from local JSON files as fallback.

    Returns:
        List of agent dicts with id, name, description.
    """
    agents_dir = Path(__file__).resolve().parent.parent / "elastic" / "agents"
    agents: List[Dict[str, Any]] = []
    if agents_dir.is_dir():
        for fp in sorted(agents_dir.glob("*.json")):
            try:
                raw = json.loads(fp.read_text(encoding="utf-8"))
                agents.append({
                    "id": raw.get("id", fp.stem),
                    "name": raw.get("display_name", raw.get("name", fp.stem)),
                    "description": raw.get("description", ""),
                    "labels": raw.get("labels", []),
                    "source": "local_config",
                })
            except (json.JSONDecodeError, OSError) as exc:
                logger.debug("Skipping agent file %s: %s", fp, exc)
    return agents


def _load_local_tools() -> List[Dict[str, Any]]:
    """Load tool definitions from local JSON files as fallback.

    Returns:
        List of tool dicts with id, type, description.
    """
    tools_dir = Path(__file__).resolve().parent.parent / "elastic" / "tools"
    tools: List[Dict[str, Any]] = []
    if tools_dir.is_dir():
        for fp in sorted(tools_dir.glob("*.json")):
            try:
                raw = json.loads(fp.read_text(encoding="utf-8"))
                tools.append({
                    "id": raw.get("id", fp.stem),
                    "name": raw.get("name", fp.stem),
                    "type": raw.get("type", "esql"),
                    "description": raw.get("description", ""),
                    "source": "local_config",
                })
            except (json.JSONDecodeError, OSError) as exc:
                logger.debug("Skipping tool file %s: %s", fp, exc)
    return tools


def list_agents() -> List[Dict[str, Any]]:
    """List all registered agents from Agent Builder.

    Tries the Kibana API first; falls back to local JSON configs.

    Returns:
        List of agent dicts with id, name, description.
    """
    base = _base_url()
    if base and _api_key():
        try:
            resp = requests.get(
                f"{base}/api/agent_builder/agents",
                headers=_headers(),
                timeout=15,
            )
            if resp.status_code == 200:
                agents = _extract_list(resp.json(), ("agents", "items", "data"))
                if agents:
                    return agents
            else:
                logger.warning(
                    "Agent Builder agents endpoint returned %d: %s",
                    resp.status_code,
                    resp.text[:200],
                )
        except requests.RequestException as exc:
            logger.warning("Failed to list agents via API: %s", exc)

    local = _load_local_agents()
    if local:
        logger.info("Using %d agents from local config files", len(local))
    return local


def list_tools() -> List[Dict[str, Any]]:
    """List all registered tools from Agent Builder.

    Tries the Kibana API first; falls back to local JSON configs.

    Returns:
        List of tool dicts with id, type, description.
    """
    base = _base_url()
    if base and _api_key():
        try:
            resp = requests.get(
                f"{base}/api/agent_builder/tools",
                headers=_headers(),
                timeout=15,
            )
            if resp.status_code == 200:
                tools = _extract_list(resp.json(), ("tools", "items", "data"))
                if tools:
                    return tools
            else:
                logger.warning(
                    "Agent Builder tools endpoint returned %d: %s",
                    resp.status_code,
                    resp.text[:200],
                )
        except requests.RequestException as exc:
            logger.warning("Failed to list tools via API: %s", exc)

    local = _load_local_tools()
    if local:
        logger.info("Using %d tools from local config files", len(local))
    return local


def _validate_response(data: Dict[str, Any], agent_id: str) -> Dict[str, Any]:
    """Validate that an agent response contains expected fields.

    Args:
        data: Parsed JSON response.
        agent_id: Agent identifier for logging.

    Returns:
        The validated data dict (unchanged if valid).
    """
    if not isinstance(data, dict):
        logger.warning("Agent %s returned non-dict: %s", agent_id, type(data).__name__)
        return {"raw_response": data, "confidence_score": 0.0}
    if "confidence_score" not in data:
        logger.debug("Agent %s response missing confidence_score, defaulting to 0.5", agent_id)
        data["confidence_score"] = 0.5
    return data


def _extract_content(raw: Dict[str, Any]) -> str:
    """Extract the text content from a converse API response.

    The converse API returns different shapes depending on version.
    We handle the common patterns here.

    Args:
        raw: Raw JSON response from the converse endpoint.

    Returns:
        Extracted text content string.
    """
    if "output" in raw:
        out = raw["output"]
        if isinstance(out, str):
            return out
        if isinstance(out, dict):
            return out.get("content", out.get("text", json.dumps(out)))

    if "response" in raw:
        resp = raw["response"]
        if isinstance(resp, str):
            return resp
        if isinstance(resp, dict):
            return resp.get("message", resp.get("content", resp.get("text", json.dumps(resp))))

    if "message" in raw:
        msg = raw["message"]
        if isinstance(msg, str):
            return msg
        if isinstance(msg, dict):
            return msg.get("content", msg.get("text", json.dumps(msg)))

    if "content" in raw:
        return raw["content"] if isinstance(raw["content"], str) else json.dumps(raw["content"])

    return json.dumps(raw)


def call_agent(
    agent_id: str,
    message: str,
    *,
    conversation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Call an Elastic Agent Builder agent via the converse API.

    Uses POST /api/agent_builder/converse with the correct payload format.

    Args:
        agent_id: The agent ID from Kibana Agent Builder.
        message: User message content to send.
        conversation_id: Optional conversation ID for multi-turn.

    Returns:
        Parsed JSON response from the agent.

    Raises:
        ValueError: If KIBANA_URL or ELASTIC_API_KEY are missing.
        requests.HTTPError: On non-2xx response after retries.
    """
    base = _base_url()
    key = _api_key()
    if not base or not key:
        raise ValueError("KIBANA_URL and ELASTIC_API_KEY must be set in .env")

    url = f"{base}/api/agent_builder/converse"
    payload: Dict[str, Any] = {
        "input": message,
        "agent_id": agent_id,
    }
    if conversation_id:
        payload["conversation_id"] = conversation_id

    last_exc: Optional[Exception] = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            logger.info("Agent Builder converse [%s] attempt %d — %s",
                        agent_id, attempt + 1, message[:80])
            resp = requests.post(url, headers=_headers(), json=payload, timeout=_TIMEOUT_S)
            resp.raise_for_status()

            raw = resp.json()
            content = _extract_content(raw)

            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                logger.debug("Agent %s returned non-JSON text, wrapping", agent_id)
                parsed = {"raw_text": content, "confidence_score": 0.5}

            validated = _validate_response(parsed, agent_id)
            logger.info("Agent %s responded with %d keys (confidence=%.2f)",
                        agent_id, len(validated),
                        validated.get("confidence_score", 0))
            return validated

        except requests.HTTPError as exc:
            last_exc = exc
            logger.warning("Agent %s HTTP error: %s", agent_id, exc)
        except requests.ConnectionError as exc:
            last_exc = exc
            logger.warning("Agent %s connection error: %s", agent_id, exc)
        except requests.Timeout as exc:
            last_exc = exc
            logger.warning("Agent %s timeout", agent_id)
        except requests.RequestException as exc:
            last_exc = exc
            logger.error("Agent %s request failed: %s", agent_id, exc)

        if attempt < _MAX_RETRIES:
            logger.info("Retrying agent %s in %ds...", agent_id, _RETRY_DELAY_S)
            time.sleep(_RETRY_DELAY_S)

    logger.error("Agent %s failed after %d attempts", agent_id, _MAX_RETRIES + 1)
    raise last_exc or RuntimeError(f"Agent {agent_id} call failed")


def converse(
    message: str,
    *,
    agent_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Direct converse call returning the full API response (for the chat UI).

    Unlike call_agent which tries to parse structured JSON, this returns the
    raw text suitable for display in a conversational interface.

    Args:
        message: User message text.
        agent_id: Optional agent ID (defaults to Elastic default agent).
        conversation_id: Optional conversation ID for multi-turn.

    Returns:
        Dict with 'response', 'conversation_id', and 'agent_id'.

    Raises:
        ValueError: If KIBANA_URL or ELASTIC_API_KEY are missing.
        requests.RequestException: On network or HTTP errors.
    """
    base = _base_url()
    if not base or not _api_key():
        raise ValueError("KIBANA_URL and ELASTIC_API_KEY must be set in .env")

    url = f"{base}/api/agent_builder/converse"
    payload: Dict[str, Any] = {"input": message}
    if agent_id:
        payload["agent_id"] = agent_id
    if conversation_id:
        payload["conversation_id"] = conversation_id

    resp = requests.post(url, headers=_headers(), json=payload, timeout=_TIMEOUT_S)
    resp.raise_for_status()
    raw = resp.json()

    return {
        "response": _extract_content(raw),
        "conversation_id": raw.get("conversation_id", raw.get("conversationId", "")),
        "agent_id": agent_id or "elastic-ai-agent",
        "raw": raw,
    }


def converse_stream(
    message: str,
    *,
    agent_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
) -> Generator[Dict[str, Any], None, None]:
    """Stream a conversation using the async converse endpoint (SSE).

    Args:
        message: User message text.
        agent_id: Optional agent ID.
        conversation_id: Optional conversation ID for multi-turn.

    Yields:
        Dicts with event data parsed from SSE stream.
    """
    base = _base_url()
    if not base or not _api_key():
        raise ValueError("KIBANA_URL and ELASTIC_API_KEY must be set in .env")

    url = f"{base}/api/agent_builder/converse/async"
    payload: Dict[str, Any] = {"input": message}
    if agent_id:
        payload["agent_id"] = agent_id
    if conversation_id:
        payload["conversation_id"] = conversation_id

    resp = requests.post(url, headers=_headers(), json=payload, timeout=_TIMEOUT_S, stream=True)
    resp.raise_for_status()

    buffer = ""
    for chunk in resp.iter_content(chunk_size=None, decode_unicode=True):
        buffer += chunk
        while "\n\n" in buffer:
            event_str, buffer = buffer.split("\n\n", 1)
            for line in event_str.split("\n"):
                if line.startswith("data: "):
                    data_str = line[6:]
                    try:
                        yield json.loads(data_str)
                    except json.JSONDecodeError:
                        yield {"raw_text": data_str}


def call_agent_streaming(
    agent_id: str,
    message: str,
) -> List[Dict[str, Any]]:
    """Call an agent and collect reasoning trace events.

    Args:
        agent_id: The agent ID from Kibana Agent Builder.
        message: User message content to send.

    Returns:
        List of reasoning trace dicts with keys: thought, tool_called, query, result_summary.
    """
    reasoning_trace: List[Dict[str, Any]] = []

    reasoning_trace.append({
        "thought": f"Sending prompt to Agent Builder agent {agent_id}",
        "tool_called": None,
        "query": None,
        "result_summary": None,
    })

    result = call_agent(agent_id, message)

    if "anomalies" in result:
        tools_used: set[str] = set()
        for ano in result.get("anomalies", []):
            tool = ano.get("tool_used", "")
            if tool and tool not in tools_used:
                tools_used.add(tool)
                reasoning_trace.append({
                    "thought": f"Running {tool} to detect {ano.get('type', 'anomalies')}",
                    "tool_called": tool,
                    "query": f"ES|QL query from {tool} tool definition",
                    "result_summary": ano.get("raw_data_summary", ""),
                })

    reasoning_trace.append({
        "thought": f"Agent completed with confidence {result.get('confidence_score', 0):.2f}",
        "tool_called": None,
        "query": None,
        "result_summary": result.get("summary", ""),
    })

    return reasoning_trace


class AgentCaller:
    """Wrapper for Agent Builder API with connection management."""

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None) -> None:
        self._custom_base = base_url
        self._custom_key = api_key

    def is_connected(self) -> bool:
        """Check if Agent Builder is reachable.

        Returns:
            True if connected.
        """
        status = test_connection()
        return status.get("connected", False)

    def chat(self, agent_id: str, message: str) -> Dict[str, Any]:
        """Send a chat message to an agent and return parsed JSON.

        Args:
            agent_id: Agent Builder agent ID.
            message: Message to send.

        Returns:
            Parsed JSON response.
        """
        return call_agent(agent_id, message)
