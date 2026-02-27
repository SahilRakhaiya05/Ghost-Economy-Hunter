"""Provision Ghost Economy Hunter agents into Elastic Agent Builder.

Reads agent configs from elastic/agents/ and tool configs from elastic/tools/,
then registers them via the Kibana Agent Builder REST API.

Correct API endpoints (per Elastic docs):
  POST /api/agent_builder/tools   — create ES|QL tools
  POST /api/agent_builder/agents  — create agents with instructions & tool assignments

Usage:
    python -m elastic.setup.provision_agents
    python -m elastic.setup.provision_agents --verify
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ghost-provision")

_KIBANA_URL = os.getenv("KIBANA_URL", "")
_API_KEY = os.getenv("ELASTIC_API_KEY", "")

AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
TOOLS_DIR = Path(__file__).resolve().parent.parent / "tools"
WORKFLOWS_DIR = Path(__file__).resolve().parent.parent / "workflows"

AGENT_FILES = [
    "cartographer.json",
    "pattern-seeker.json",
    "valuator.json",
    "action-taker.json",
]

TOOL_FILES = [
    "usage-anomaly.json",
    "runtime-anomaly.json",
    "energy-anomaly.json",
    "value-calculator.json",
]

TOOL_AGENT_MAP: Dict[str, List[str]] = {
    "ghost-cartographer": [],
    "ghost-pattern-seeker": ["ghost-usage-anomaly", "ghost-runtime-anomaly", "ghost-energy-anomaly"],
    "ghost-valuator": ["ghost-value-calculator"],
    "ghost-action-taker": [],
}


def _headers() -> Dict[str, str]:
    """Build auth headers for Kibana API.

    Returns:
        Dict of HTTP headers.
    """
    return {
        "Authorization": f"ApiKey {_API_KEY}",
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
    }


def _load_json(path: Path) -> Dict[str, Any]:
    """Load and parse a JSON file.

    Args:
        path: Path to the JSON file.

    Returns:
        Parsed dict.
    """
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def register_tool(tool_config: Dict[str, Any]) -> Optional[str]:
    """Register an ES|QL tool in Agent Builder.

    Uses POST /api/agent_builder/tools with the correct payload:
    {id, type, description, tags, configuration: {query, params}}.

    Args:
        tool_config: Tool definition dict from JSON file.

    Returns:
        Tool ID if created, None on failure.
    """
    url = f"{_KIBANA_URL.rstrip('/')}/api/agent_builder/tools"
    tool_id = tool_config.get("id", tool_config["name"].replace("_", "-"))

    configuration: Dict[str, Any] = {
        "query": tool_config["query"],
    }
    if tool_config.get("params"):
        configuration["params"] = tool_config["params"]

    payload: Dict[str, Any] = {
        "id": tool_id,
        "type": tool_config.get("type", "esql"),
        "description": tool_config["description"],
        "configuration": configuration,
    }
    if "tags" in tool_config:
        payload["tags"] = tool_config["tags"]

    try:
        resp = requests.post(url, headers=_headers(), json=payload, timeout=30)
        if resp.status_code in (200, 201):
            created_id = resp.json().get("id", tool_id)
            log.info("  Registered tool: %s -> %s", tool_id, created_id)
            return created_id
        already_exists = (
            resp.status_code == 409
            or (resp.status_code == 400 and "already exists" in resp.text)
        )
        if already_exists:
            put_url = f"{url}/{tool_id}"
            put_payload = {k: v for k, v in payload.items() if k != "id"}
            put_resp = requests.put(put_url, headers=_headers(), json=put_payload, timeout=30)
            if put_resp.status_code in (200, 201):
                log.info("  Updated existing tool: %s", tool_id)
                return tool_id
            log.info("  Tool already exists (update skipped): %s", tool_id)
            return tool_id
        log.error("  Failed to register tool %s: %s %s",
                  tool_id, resp.status_code, resp.text[:200])
        return None
    except requests.RequestException as exc:
        log.error("  Tool registration request failed: %s", exc)
        return None


def register_agent(agent_config: Dict[str, Any], tool_ids: List[str]) -> Optional[str]:
    """Register an agent in Agent Builder with instructions and tool assignments.

    Uses POST /api/agent_builder/agents with the correct payload:
    {id, name, description, configuration: {instructions, tools: [{tool_ids: [...]}]}}.

    Args:
        agent_config: Agent definition dict from JSON file.
        tool_ids: List of tool IDs to assign to this agent.

    Returns:
        Agent ID if created, None on failure.
    """
    url = f"{_KIBANA_URL.rstrip('/')}/api/agent_builder/agents"
    agent_id = agent_config.get("id", f"ghost-{agent_config['name'].lower().replace(' ', '-')}")

    tools_block: List[Dict[str, Any]] = []
    if tool_ids:
        tools_block.append({"tool_ids": tool_ids})

    payload: Dict[str, Any] = {
        "id": agent_id,
        "name": agent_config.get("display_name", agent_config["name"]),
        "description": agent_config["description"],
        "configuration": {
            "instructions": agent_config["instructions"],
            "tools": tools_block,
        },
    }
    if "labels" in agent_config:
        payload["labels"] = agent_config["labels"]

    try:
        resp = requests.post(url, headers=_headers(), json=payload, timeout=30)
        if resp.status_code in (200, 201):
            created_id = resp.json().get("id", agent_id)
            log.info("  Registered agent: %s -> %s", agent_config["name"], created_id)
            return created_id
        already_exists = (
            resp.status_code == 409
            or (resp.status_code == 400 and "already exists" in resp.text)
        )
        if already_exists:
            put_url = f"{url}/{agent_id}"
            put_payload = {k: v for k, v in payload.items() if k != "id"}
            put_resp = requests.put(put_url, headers=_headers(), json=put_payload, timeout=30)
            if put_resp.status_code in (200, 201):
                log.info("  Updated existing agent: %s -> %s", agent_config["name"], agent_id)
                return agent_id
            log.info("  Agent already exists (update skipped): %s", agent_id)
            return agent_id
        log.error("  Failed to register agent %s: %s %s",
                  agent_config["name"], resp.status_code, resp.text[:200])
        return None
    except requests.RequestException as exc:
        log.error("  Agent registration request failed: %s", exc)
        return None


def register_workflow() -> Optional[str]:
    """Register the action workflow via the Kibana Workflows API.

    Returns:
        Workflow ID if created, None on failure.
    """
    yaml_path = WORKFLOWS_DIR / "action_workflow.yaml"
    if not yaml_path.exists():
        log.warning("Workflow file not found: %s", yaml_path)
        return None

    url = f"{_KIBANA_URL.rstrip('/')}/api/workflows"
    yaml_content = yaml_path.read_text(encoding="utf-8")

    try:
        resp = requests.post(
            url,
            headers={
                **_headers(),
                "x-elastic-internal-origin": "Kibana",
            },
            json={"yaml": yaml_content},
            timeout=30,
        )
        if resp.status_code in (200, 201):
            wf_id = resp.json().get("id", "ghost-economy-action-workflow")
            log.info("  Registered workflow: %s", wf_id)
            return wf_id
        if resp.status_code == 409:
            log.info("  Workflow already exists")
            return "ghost-economy-action-workflow"
        log.warning("  Workflow registration returned %s: %s", resp.status_code, resp.text[:200])
        return None
    except requests.RequestException as exc:
        log.warning("  Workflow registration failed (non-critical): %s", exc)
        return None


def verify_agent(agent_id: str, agent_name: str) -> bool:
    """Send a test message to an agent and verify it responds.

    Args:
        agent_id: Agent Builder agent ID.
        agent_name: Human-readable agent name.

    Returns:
        True if the agent responded.
    """
    from orchestrator.agent_caller import call_agent
    try:
        result = call_agent(agent_id, "Respond with a short JSON status check.")
        if result and isinstance(result, dict):
            log.info("  VERIFIED: %s responded with %d keys", agent_name, len(result))
            return True
    except (ValueError, requests.RequestException, json.JSONDecodeError) as exc:
        log.error("  VERIFY FAILED for %s: %s", agent_name, exc)
    return False


def provision_all(do_verify: bool = False) -> Dict[str, str]:
    """Register all tools, agents, and workflows in Agent Builder.

    Args:
        do_verify: If True, send test messages to each agent after provisioning.

    Returns:
        Dict mapping agent display name to agent ID.
    """
    if not _KIBANA_URL or not _API_KEY:
        log.error("KIBANA_URL and ELASTIC_API_KEY must be set in .env")
        sys.exit(1)

    log.info("=" * 60)
    log.info("PROVISIONING GHOST ECONOMY HUNTER AGENTS")
    log.info("Kibana: %s", _KIBANA_URL)
    log.info("=" * 60)

    log.info("\nStep 1: Registering ES|QL tools...")
    registered_tool_ids: List[str] = []
    for fname in TOOL_FILES:
        path = TOOLS_DIR / fname
        if not path.exists():
            log.warning("Tool file not found: %s", path)
            continue
        config = _load_json(path)
        tid = register_tool(config)
        if tid:
            registered_tool_ids.append(tid)

    log.info("\nStep 2: Registering agents...")
    agent_ids: Dict[str, str] = {}
    for fname in AGENT_FILES:
        path = AGENTS_DIR / fname
        if not path.exists():
            log.warning("Agent file not found: %s", path)
            continue
        config = _load_json(path)
        agent_id = config.get("id", f"ghost-{config['name'].lower().replace(' ', '-')}")
        assigned = TOOL_AGENT_MAP.get(agent_id, [])
        aid = register_agent(config, assigned)
        if aid:
            agent_ids[config["name"]] = aid

    log.info("\nStep 3: Registering workflow...")
    register_workflow()

    log.info("\nProvisioned %d tools and %d agents", len(registered_tool_ids), len(agent_ids))

    if do_verify and agent_ids:
        log.info("\nStep 4: Verifying agents respond...")
        for name, aid in agent_ids.items():
            verify_agent(aid, name)

    log.info("\n--- Agent IDs (add to .env if needed) ---")
    for name, aid in agent_ids.items():
        env_key = f"AGENT_{name.upper().replace(' ', '_')}_ID"
        log.info("  %s=%s", env_key, aid)

    return agent_ids


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Provision Ghost Economy Hunter agents")
    parser.add_argument("--verify", action="store_true", help="Verify agents respond after provisioning")
    args = parser.parse_args()
    provision_all(do_verify=args.verify)


if __name__ == "__main__":
    main()
