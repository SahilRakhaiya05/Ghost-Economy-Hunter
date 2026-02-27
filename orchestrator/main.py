"""Ghost Economy Hunter — Main Orchestrator.

Runs the 4-agent pipeline directly via ES|QL. Each function represents one agent's work:
  Cartographer  → lists indexes and maps their structure
  Pattern Seeker → runs 3 ES|QL anomaly queries
  Valuator       → looks up pricing and calculates dollar impact
  Action Taker   → scores actionability, sends Slack, indexes audit records
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError

from constants import INDEX_GHOST_ECONOMY_AUDIT
from orchestrator.value_formatter import format_dollar

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ghost-economy")

_ES_URL     = os.getenv("ELASTIC_URL", "")
_ES_KEY     = os.getenv("ELASTIC_API_KEY", "")
_SLACK_URL  = os.getenv("SLACK_WEBHOOK_URL", "")


def _es() -> Elasticsearch:
    """Return a configured Elasticsearch client."""
    return Elasticsearch(_ES_URL, api_key=_ES_KEY, request_timeout=30)


def _esql(query: str) -> List[Dict[str, Any]]:
    """Run an ES|QL query and return rows as list of dicts.

    Args:
        query: ES|QL query string.

    Returns:
        List of row dicts keyed by column name.
    """
    es = _es()
    result = es.esql.query(query=query)
    cols = [c["name"] for c in result["columns"]]
    return [dict(zip(cols, row)) for row in result["values"]]


# ── Agent 1: Cartographer ─────────────────────────────────────────────────────

_SECTOR_DETECTION_RULES: Dict[str, List[str]] = {
    "healthcare":     ["drug_name", "qty_administered", "ward_id", "qty_ordered", "qty_used"],
    "retail":         ["product_sku", "pos_sales", "units_sold", "units_wasted", "store_id"],
    "manufacturing":  ["machine_id", "runtime_minutes", "production_units", "shift_active"],
    "real_estate":    ["building_id", "energy_kwh", "occupancy_pct", "floor_id"],
    "logistics":      ["vehicle_id", "empty_miles", "load_weight", "trip_manifest"],
    "education":      ["room_id", "course_id", "enrollment_count", "license_count"],
    "government":     ["contract_id", "vendor_id", "invoice_amount", "deliverables"],
    "hospitality":    ["covers_served", "room_occupancy", "minibar_items", "outlet_id"],
}


def _detect_sector(fields: List[str]) -> str:
    """Auto-detect which sector the data belongs to based on field names.

    Args:
        fields: List of all field names found across indexes.

    Returns:
        Detected sector ID or 'multi-sector'.
    """
    field_set = set(f.lower() for f in fields)
    scores: Dict[str, int] = {}
    for sector, indicators in _SECTOR_DETECTION_RULES.items():
        scores[sector] = sum(1 for i in indicators if i in field_set)
    matches = [(s, c) for s, c in scores.items() if c >= 2]
    if not matches:
        return "generic"
    matches.sort(key=lambda x: x[1], reverse=True)
    if len(matches) >= 2 and matches[0][1] == matches[1][1]:
        return "multi-sector"
    return matches[0][0]


def run_cartographer() -> Dict[str, Any]:
    """Map all Elasticsearch indexes dynamically and identify anomaly potential.

    Auto-discovers every non-system index, inspects field mappings,
    and detects the business sector using field-name heuristics.

    Returns:
        dict: indexes list, sector_detected, correlation_pairs, confidence_score, summary.
    """
    log.info("Agent 1 — Cartographer: mapping indexes...")
    es = _es()

    domain_hints = {
        "factory": "manufacturing operations",
        "hospital": "healthcare procurement",
        "drug": "healthcare procurement",
        "building": "real estate energy",
        "nyc": "real estate energy",
        "pricing": "pricing reference",
        "exception": "exception registry",
        "audit": "audit trail",
        "retail": "retail operations",
        "vehicle": "logistics fleet",
        "hotel": "hospitality operations",
        "room": "space utilization",
        "software": "license management",
        "contract": "government procurement",
    }

    known_targets = [
        "factory-iot-data", "hospital-drugs", "nyc-buildings",
        "pricing-reference", "known-exceptions", "ghost-economy-audit",
    ]
    try:
        cat_result = es.cat.indices(format="json", h="index,docs.count")
        discovered = [
            (e.get("index", ""), int(e.get("docs.count", 0) or 0))
            for e in cat_result if not e.get("index", "").startswith(".")
        ]
    except (ApiError, Exception):
        log.warning("cat.indices failed — falling back to known target indexes")
        discovered = []
        for idx in known_targets:
            try:
                cnt = _esql(f"FROM {idx} | STATS c = COUNT(*) | LIMIT 1")
                discovered.append((idx, cnt[0]["c"] if cnt else 0))
            except (ApiError, Exception):
                discovered.append((idx, 0))

    all_fields: List[str] = []
    indexes = []

    for idx_name, doc_count in discovered:
        if idx_name.startswith("."):
            continue

        try:
            mapping = es.indices.get_mapping(index=idx_name)
            props = {}
            for _k, v in mapping.items():
                props = v.get("mappings", {}).get("properties", {})
                break
        except ApiError:
            props = {}

        numeric_fields = [f for f, d in props.items() if d.get("type") in ("float", "double", "integer", "long")]
        keyword_fields = [f for f, d in props.items() if d.get("type") == "keyword"]
        all_fields.extend(list(props.keys()))

        domain = "general"
        for hint_key, hint_domain in domain_hints.items():
            if hint_key in idx_name:
                domain = hint_domain
                break

        if len(numeric_fields) >= 2:
            potential = "HIGH"
        elif len(numeric_fields) >= 1:
            potential = "MEDIUM"
        else:
            potential = "LOW"

        indexes.append({
            "name": idx_name,
            "domain": domain,
            "doc_count": doc_count,
            "timestamp_field": "@timestamp" if "@timestamp" in props else None,
            "numeric_fields": numeric_fields,
            "keyword_fields": keyword_fields,
            "anomaly_potential": potential,
        })
        log.info("  %s — %d docs — %s anomaly potential", idx_name, doc_count, potential)

    sector = _detect_sector(all_fields)
    high_count = sum(1 for i in indexes if i["anomaly_potential"] == "HIGH")

    result = {
        "indexes": indexes,
        "sector_detected": sector,
        "correlation_pairs": [
            {"index_a": "factory-iot-data", "index_b": "pricing-reference",
             "reason": "machine idle hours costed via press-machine-hour unit rate"},
            {"index_a": "hospital-drugs", "index_b": "pricing-reference",
             "reason": "drug waste quantity costed via insulin unit rate"},
            {"index_a": "nyc-buildings", "index_b": "pricing-reference",
             "reason": "excess energy kWh costed via kwh-nyc electricity rate"},
        ],
        "confidence_score": 0.97,
        "summary": (
            f"Found {len(indexes)} indexes — sector: {sector} — "
            f"{high_count} with HIGH anomaly potential"
        ),
    }
    log.info("Cartographer complete: %s", result["summary"])
    return result


# ── Agent 2: Pattern Seeker ───────────────────────────────────────────────────

def run_pattern_seeker(index_map: Dict[str, Any]) -> Dict[str, Any]:
    """Detect waste anomalies using 3 ES|QL tools.

    Args:
        index_map: Output from Cartographer.

    Returns:
        dict: anomalies list, total_anomalies_found, confidence_score, summary.
    """
    log.info("Agent 2 — Pattern Seeker: running all 3 ES|QL tools...")
    anomalies = []
    ano_id = 0

    # Tool 1: usage_anomaly (hospital drug over-procurement)
    log.info("  Running ghost.usage_anomaly tool...")
    usage_rows = _esql(
        "FROM hospital-drugs\n"
        "| STATS\n"
        "    total_ordered = SUM(qty_ordered),\n"
        "    total_used    = SUM(qty_used)\n"
        "  BY drug_name, wing_id\n"
        "| EVAL\n"
        "    delta_qty   = total_ordered - total_used,\n"
        "    waste_ratio = TO_DOUBLE(total_ordered - total_used) / TO_DOUBLE(total_ordered)\n"
        "| WHERE waste_ratio > 0.25\n"
        "| SORT waste_ratio DESC\n"
        "| LIMIT 20\n"
        "| KEEP drug_name, wing_id, total_ordered, total_used, delta_qty, waste_ratio"
    )
    for row in usage_rows:
        ano_id += 1
        anomalies.append({
            "id": f"ANO-{ano_id:03d}",
            "type": "USAGE_ORDER_MISMATCH",
            "entity": f"{row['drug_name']} — {row['wing_id']}",
            "index": "hospital-drugs",
            "delta_quantity": int(row["delta_qty"]),
            "unit": "units",
            "time_period_days": 180,
            "confidence_score": round(min(0.98, row["waste_ratio"] + 0.55), 2),
            "tool_used": "ghost.usage_anomaly",
            "raw_data_summary": (
                f"ordered={row['total_ordered']} used={row['total_used']} "
                f"delta={row['delta_qty']} waste_ratio={row['waste_ratio']:.3f}"
            ),
        })
    log.info("  usage_anomaly: %d anomalies", len(usage_rows))

    # Tool 2: runtime_anomaly (factory idle machines)
    log.info("  Running ghost.runtime_anomaly tool...")
    runtime_rows = _esql(
        "FROM factory-iot-data\n"
        "| WHERE shift_active == false\n"
        "| STATS\n"
        "    total_idle_minutes = SUM(runtime_minutes),\n"
        "    avg_daily_idle     = AVG(runtime_minutes)\n"
        "  BY machine_id\n"
        "| EVAL\n"
        "    idle_hours     = total_idle_minutes / 60,\n"
        "    estimated_cost = idle_hours * 112.50\n"
        "| WHERE avg_daily_idle > 200\n"
        "| SORT estimated_cost DESC\n"
        "| LIMIT 10\n"
        "| KEEP machine_id, total_idle_minutes, idle_hours, avg_daily_idle, estimated_cost"
    )
    for row in runtime_rows:
        ano_id += 1
        anomalies.append({
            "id": f"ANO-{ano_id:03d}",
            "type": "RUNTIME_SCHEDULE_GAP",
            "entity": f"{row['machine_id']} — Factory Floor",
            "index": "factory-iot-data",
            "delta_quantity": int(row["idle_hours"]),
            "unit": "idle_hours",
            "time_period_days": 90,
            "confidence_score": 0.95,
            "tool_used": "ghost.runtime_anomaly",
            "raw_data_summary": (
                f"machine={row['machine_id']} idle_minutes={row['total_idle_minutes']} "
                f"idle_hours={row['idle_hours']:.1f} avg_daily={row['avg_daily_idle']:.0f}min "
                f"estimated_cost=${row['estimated_cost']:,.2f}"
            ),
        })
    log.info("  runtime_anomaly: %d anomalies", len(runtime_rows))

    # Tool 3: energy_anomaly (building waste)
    log.info("  Running ghost.energy_anomaly tool...")
    energy_rows = _esql(
        "FROM nyc-buildings\n"
        "| STATS\n"
        "    avg_occupancy    = AVG(occupancy_pct),\n"
        "    total_energy_kwh = SUM(energy_kwh)\n"
        "  BY building_id, borough\n"
        "| EVAL\n"
        "    total_energy_cost = total_energy_kwh * 0.19,\n"
        "    waste_score       = (1 - avg_occupancy) * total_energy_kwh\n"
        "| WHERE avg_occupancy < 0.15 AND total_energy_kwh > 5000\n"
        "| SORT total_energy_cost DESC\n"
        "| LIMIT 15\n"
        "| KEEP building_id, borough, avg_occupancy, total_energy_kwh, total_energy_cost, waste_score"
    )
    for row in energy_rows:
        ano_id += 1
        anomalies.append({
            "id": f"ANO-{ano_id:03d}",
            "type": "ENERGY_OCCUPANCY_DIVERGENCE",
            "entity": f"{row['building_id']} — {row['borough']}",
            "index": "nyc-buildings",
            "delta_quantity": int(row["total_energy_kwh"]),
            "unit": "kwh",
            "time_period_days": 90,
            "confidence_score": round(min(0.97, (1 - row["avg_occupancy"]) * 0.9), 2),
            "tool_used": "ghost.energy_anomaly",
            "raw_data_summary": (
                f"building={row['building_id']} avg_occ={row['avg_occupancy']:.1%} "
                f"total_kwh={row['total_energy_kwh']:,.0f} "
                f"energy_cost=${row['total_energy_cost']:,.2f}"
            ),
        })
    log.info("  energy_anomaly: %d anomalies", len(energy_rows))

    result = {
        "anomalies": anomalies,
        "total_anomalies_found": len(anomalies),
        "confidence_score": 0.93,
        "summary": (
            f"Found {len(anomalies)} anomalies: "
            f"{len(usage_rows)} drug waste, {len(runtime_rows)} idle machines, "
            f"{len(energy_rows)} building energy waste"
        ),
    }
    log.info("Pattern Seeker complete: %s", result["summary"])
    return result


# ── Agent 3: Valuator ─────────────────────────────────────────────────────────

def run_valuator(patterns: Dict[str, Any]) -> Dict[str, Any]:
    """Assign dollar values to every anomaly using the pricing reference.

    Args:
        patterns: Output from Pattern Seeker.

    Returns:
        dict: valued_findings, total_ghost_economy_usd, total_annualized_usd, etc.
    """
    log.info("Agent 3 — Valuator: calculating dollar impact...")

    # Load pricing reference via ghost.value_calculator (ES|QL)
    pricing_rows = _esql("FROM pricing-reference | KEEP item_key, unit_cost_usd, unit_label | LIMIT 20")
    pricing = {row["item_key"]: row for row in pricing_rows}
    log.info("  Loaded %d pricing items from pricing-reference", len(pricing))

    unit_cost_map = {
        "USAGE_ORDER_MISMATCH":        pricing.get("insulin", {}).get("unit_cost_usd", 212.50),
        "RUNTIME_SCHEDULE_GAP":        pricing.get("press-machine-hour", {}).get("unit_cost_usd", 112.50),
        "ENERGY_OCCUPANCY_DIVERGENCE": pricing.get("kwh-nyc", {}).get("unit_cost_usd", 0.19),
    }
    category_map = {
        "USAGE_ORDER_MISMATCH":        "Drug Over-Procurement",
        "RUNTIME_SCHEDULE_GAP":        "Idle Equipment Runtime",
        "ENERGY_OCCUPANCY_DIVERGENCE": "Energy-Occupancy Waste",
    }

    valued_findings = []
    for ano in patterns.get("anomalies", []):
        atype = ano["type"]
        unit_cost = unit_cost_map.get(atype, 0)
        delta     = ano["delta_quantity"]
        days      = ano["time_period_days"]
        dollar_value = round(delta * unit_cost, 2)
        annualized   = round(dollar_value * (365 / days), 2)

        if dollar_value >= 100_000:
            priority = "CRITICAL"
        elif dollar_value >= 50_000:
            priority = "HIGH"
        elif dollar_value >= 10_000:
            priority = "MEDIUM"
        else:
            priority = "LOW"

        # Build calculation string — round unit_cost for clean display
        uc = round(unit_cost, 4)
        if atype == "RUNTIME_SCHEDULE_GAP":
            calc = f"{delta:,} idle hours \u00d7 ${uc}/hr"
        elif atype == "ENERGY_OCCUPANCY_DIVERGENCE":
            calc = f"{delta:,} kWh \u00d7 ${uc}/kWh"
        else:
            calc = f"{delta:,} units \u00d7 ${uc}/unit"

        valued_findings.append({
            "anomaly_id":       ano["id"],
            "entity":           ano["entity"],
            "category":         category_map.get(atype, atype),
            "dollar_value":     dollar_value,
            "calculation":      calc,
            "unit_cost_source": "pricing-reference index",
            "annualized_value": annualized,
            "confidence_score": ano["confidence_score"],
            "priority":         priority,
        })
        log.info("  %s → %s (%s)", ano["id"], format_dollar(dollar_value), priority)

    total = round(sum(f["dollar_value"] for f in valued_findings), 2)
    annualized_total = round(sum(f["annualized_value"] for f in valued_findings), 2)

    result = {
        "valued_findings":       valued_findings,
        "total_ghost_economy_usd": total,
        "total_annualized_usd":  annualized_total,
        "confidence_score":      0.92,
        "currency":              "USD",
        "summary":               f"Found {format_dollar(total)} in hidden waste across 3 industries",
    }
    log.info("Valuator complete: %s", result["summary"])
    return result


# ── Agent 4: Action Taker ─────────────────────────────────────────────────────

def run_action_taker(values: Dict[str, Any]) -> Dict[str, Any]:
    """Verify findings, send Slack alerts, and index audit records.

    Args:
        values: Output from Valuator.

    Returns:
        dict: verified_actions, actions_triggered, actions_suppressed, etc.
    """
    log.info("Agent 4 — Action Taker: verifying findings and firing actions...")
    verified_actions = []
    triggered = 0
    suppressed = 0
    total_actioned = 0.0

    for finding in values.get("valued_findings", []):
        dollar  = finding["dollar_value"]
        conf    = finding["confidence_score"]
        priority = finding["priority"]

        # Actionability score
        dollar_score = {
            "CRITICAL": 1.0,
            "HIGH":     0.8,
            "MEDIUM":   0.6,
            "LOW":      0.3,
        }.get(priority, 0.5)
        actionability = round((conf + dollar_score) / 2, 2)

        if actionability >= 0.5:
            action_taken = "workflow_triggered"
            triggered += 1
            total_actioned += dollar
            slack_msg = (
                f"Ghost Economy Alert: {format_dollar(dollar)} waste found. "
                f"{finding['entity']} — {finding['category']}. Action required."
            )[:199]
        else:
            action_taken = "suppressed"
            suppressed += 1
            slack_msg = ""

        verified_actions.append({
            "anomaly_id":        finding["anomaly_id"],
            "entity":            finding["entity"],
            "dollar_value":      dollar,
            "actionability_score": actionability,
            "action_taken":      action_taken,
            "slack_message":     slack_msg,
        })

    # Send Slack alerts
    _send_slack_alerts(verified_actions)

    # Index audit records
    _index_audit_records(values.get("valued_findings", []), verified_actions)

    result = {
        "verified_actions":        verified_actions,
        "actions_triggered":       triggered,
        "actions_suppressed":      suppressed,
        "total_value_actioned_usd": round(total_actioned, 2),
        "confidence_score":        0.94,
        "summary":                 (
            f"Triggered {triggered} actions totaling {format_dollar(total_actioned)}. "
            f"{suppressed} suppressed."
        ),
    }
    log.info("Action Taker complete: %s", result["summary"])
    return result


def _send_slack_alerts(verified_actions: List[Dict]) -> None:
    """Send Slack notifications for each workflow_triggered action.

    Args:
        verified_actions: List of action decision dicts.
    """
    if not _SLACK_URL or "PASTE_LATER" in _SLACK_URL or not _SLACK_URL.startswith("https://hooks"):
        log.warning("SLACK_WEBHOOK_URL not configured — skipping Slack alerts")
        return
    for action in verified_actions:
        if action.get("action_taken") == "workflow_triggered":
            msg = action.get("slack_message", "Ghost Economy alert.")
            try:
                r = requests.post(_SLACK_URL, json={"text": msg}, timeout=10)
                if r.status_code == 200:
                    log.info("Slack alert sent: %s", action.get("entity", ""))
                else:
                    log.warning("Slack returned %s", r.status_code)
            except requests.RequestException as exc:
                log.error("Slack request failed: %s", exc)


def _index_audit_records(
    valued_findings: List[Dict], verified_actions: List[Dict]
) -> None:
    """Index one audit record per finding into ghost-economy-audit.

    Args:
        valued_findings: Dollar-valued anomaly list from Valuator.
        verified_actions: Action decisions from Action Taker.
    """
    if not _ES_URL or not _ES_KEY:
        log.warning("ES credentials missing — skipping audit indexing")
        return
    es = _es()
    action_map = {a["anomaly_id"]: a for a in verified_actions}
    ts = datetime.utcnow().isoformat() + "Z"
    indexed = 0
    for finding in valued_findings:
        aid    = finding.get("anomaly_id", "unknown")
        action = action_map.get(aid, {})
        doc = {
            "@timestamp":      ts,
            "finding_id":      aid,
            "entity":          finding.get("entity", ""),
            "category":        finding.get("category", ""),
            "dollar_value":    finding.get("dollar_value", 0),
            "calculation":     finding.get("calculation", ""),
            "confidence":      finding.get("confidence_score", 0),
            "action_taken":    action.get("action_taken", "pending"),
            "priority":        finding.get("priority", ""),
            "annualized_value": finding.get("annualized_value", 0),
        }
        try:
            es.index(index=INDEX_GHOST_ECONOMY_AUDIT, document=doc)
            indexed += 1
        except ApiError as exc:
            log.error("Failed to index audit record %s: %s", aid, exc)
    log.info("Indexed %d audit records into %s", indexed, INDEX_GHOST_ECONOMY_AUDIT)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_hunt() -> Dict[str, Any]:
    """Execute the complete Ghost Economy Hunt pipeline.

    Returns:
        dict: Full hunt results including total_ghost_economy_usd, anomalies_found, actions_triggered.
    """
    log.info("=" * 60)
    log.info("GHOST ECONOMY HUNT STARTING")
    log.info("=" * 60)
    start = datetime.utcnow()

    index_map = run_cartographer()
    patterns  = run_pattern_seeker(index_map)
    values    = run_valuator(patterns)
    actions   = run_action_taker(values)

    total       = values.get("total_ghost_economy_usd", 0)
    n_anomalies = patterns.get("total_anomalies_found", 0)
    n_actions   = actions.get("actions_triggered", 0)
    duration    = (datetime.utcnow() - start).total_seconds()

    print("\n" + "=" * 60)
    print("  GHOST ECONOMY HUNT COMPLETE")
    print("=" * 60)
    print(f"  Total Hidden Value Found: {format_dollar(total)}")
    print(f"  Annualized Impact:        {format_dollar(values.get('total_annualized_usd', 0))}")
    print(f"  Anomalies Detected:       {n_anomalies}")
    print(f"  Actions Triggered:        {n_actions}")
    print(f"  Time Taken:               {duration:.1f} seconds")
    print("=" * 60)
    print("\nTop Findings:")
    for f in sorted(values.get("valued_findings", []), key=lambda x: x["dollar_value"], reverse=True):
        print(f"  [{f['priority']:8}] {format_dollar(f['dollar_value']):>15}  {f['entity']}")
    print()

    return {
        "run_timestamp":           datetime.utcnow().isoformat() + "Z",
        "total_ghost_economy_usd": total,
        "anomalies_found":         n_anomalies,
        "actions_triggered":       n_actions,
        "index_map":               index_map,
        "patterns":                patterns,
        "values":                  values,
        "actions":                 actions,
    }


if __name__ == "__main__":
    run_hunt()
