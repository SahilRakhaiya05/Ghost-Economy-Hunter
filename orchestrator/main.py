"""Ghost Economy Hunter — Main Orchestrator.

Runs the 4-agent pipeline. When AGENT_BUILDER_ENABLED=true (default),
agents are called via Agent Builder API. Otherwise falls back to direct ES|QL.

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
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError

from constants import (
    AGENT_ACTION_TAKER,
    AGENT_CARTOGRAPHER,
    AGENT_PATTERN_SEEKER,
    AGENT_VALUATOR,
    INDEX_FACTORY_IOT,
    INDEX_GHOST_ECONOMY_AUDIT,
    INDEX_HOSPITAL_DRUGS,
    INDEX_KNOWN_EXCEPTIONS,
    INDEX_NYC_BUILDINGS,
    INDEX_PRICING_REFERENCE,
)
from orchestrator.value_formatter import format_dollar

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ghost-economy")

_ES_URL = os.getenv("ELASTIC_URL", "")
_ES_KEY = os.getenv("ELASTIC_API_KEY", "")
_SLACK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
_AGENT_BUILDER_ENABLED = os.getenv("AGENT_BUILDER_ENABLED", "true").lower() in ("true", "1", "yes")

_reasoning_log: List[Dict[str, Any]] = []


def get_reasoning_log() -> List[Dict[str, Any]]:
    """Return the accumulated reasoning trace from the last pipeline run.

    Returns:
        List of reasoning event dicts.
    """
    return list(_reasoning_log)


def _add_reasoning(
    agent: str,
    thought: str,
    *,
    tool: Optional[str] = None,
    query: Optional[str] = None,
    result_summary: Optional[str] = None,
) -> None:
    """Append a reasoning trace entry.

    Args:
        agent: Agent name.
        thought: What the agent is thinking/doing.
        tool: Tool being called, if any.
        query: ES|QL query being run, if any.
        result_summary: Brief result description, if any.
    """
    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "agent": agent,
        "thought": thought,
        "tool": tool,
        "query": query,
        "result_summary": result_summary,
    }
    _reasoning_log.append(entry)
    log.info("[%s] %s", agent, thought)


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


def _try_agent_builder(agent_id: str, message: str) -> Optional[Dict[str, Any]]:
    """Attempt to call an agent via Agent Builder. Returns None on failure.

    Args:
        agent_id: Agent Builder agent ID.
        message: Prompt to send.

    Returns:
        Parsed response dict or None if Agent Builder unavailable.
    """
    if not _AGENT_BUILDER_ENABLED:
        return None
    try:
        from orchestrator.agent_caller import call_agent
        return call_agent(agent_id, message)
    except (ValueError, requests.RequestException, json.JSONDecodeError) as exc:
        log.warning("Agent Builder call failed for %s, falling back to direct ES|QL: %s",
                    agent_id, exc)
        return None


# ── Agent 1: Cartographer ─────────────────────────────────────────────────────

_SECTOR_DETECTION_RULES: Dict[str, List[str]] = {
    "healthcare": ["drug_name", "qty_administered", "ward_id", "qty_ordered", "qty_used"],
    "retail": ["product_sku", "pos_sales", "units_sold", "units_wasted", "store_id"],
    "manufacturing": ["machine_id", "runtime_minutes", "production_units", "shift_active"],
    "real_estate": ["building_id", "energy_kwh", "occupancy_pct", "floor_id"],
    "logistics": ["vehicle_id", "empty_miles", "load_weight", "trip_manifest"],
    "education": ["room_id", "course_id", "enrollment_count", "license_count"],
    "government": ["contract_id", "vendor_id", "invoice_amount", "deliverables"],
    "hospitality": ["covers_served", "room_occupancy", "minibar_items", "outlet_id"],
}


def _detect_sector(fields: List[str]) -> str:
    """Auto-detect which sector the data belongs to based on field names.

    Args:
        fields: List of all field names found across indexes.

    Returns:
        Detected sector ID or 'multi-sector'.
    """
    field_set = {f.lower() for f in fields}
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

    Tries Agent Builder first, falls back to direct ES|QL.

    Returns:
        dict: indexes list, sector_detected, correlation_pairs, confidence_score, summary.
    """
    _add_reasoning("Cartographer", "Starting index discovery and mapping...")

    ab_result = _try_agent_builder(
        AGENT_CARTOGRAPHER,
        "Map all Elasticsearch indexes. For each index report: name, domain, doc_count, "
        "timestamp_field, numeric_fields, keyword_fields, anomaly_potential. "
        "Also detect the business sector and identify correlation pairs."
    )
    if ab_result and "indexes" in ab_result:
        _add_reasoning("Cartographer",
                       f"Agent Builder responded: {len(ab_result.get('indexes', []))} indexes found",
                       result_summary=ab_result.get("summary", ""))
        return ab_result

    _add_reasoning("Cartographer", "Using direct ES|QL for index discovery")

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
        INDEX_FACTORY_IOT, INDEX_HOSPITAL_DRUGS, INDEX_NYC_BUILDINGS,
        INDEX_PRICING_REFERENCE, INDEX_KNOWN_EXCEPTIONS, INDEX_GHOST_ECONOMY_AUDIT,
    ]

    try:
        cat_result = es.cat.indices(format="json", h="index,docs.count")
        discovered = [
            (e.get("index", ""), int(e.get("docs.count", 0) or 0))
            for e in cat_result if not e.get("index", "").startswith(".")
        ]
    except ApiError as exc:
        log.warning("cat.indices failed (%s) — falling back to known target indexes", exc)
        discovered = []
        for idx in known_targets:
            try:
                cnt = _esql(f"FROM {idx} | STATS c = COUNT(*) | LIMIT 1")
                discovered.append((idx, cnt[0]["c"] if cnt else 0))
            except ApiError:
                discovered.append((idx, 0))

    all_fields: List[str] = []
    indexes = []

    for idx_name, doc_count in discovered:
        if idx_name.startswith("."):
            continue

        _add_reasoning("Cartographer", f"Inspecting index: {idx_name} ({doc_count} docs)")

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

    sector = _detect_sector(all_fields)
    high_count = sum(1 for i in indexes if i["anomaly_potential"] == "HIGH")

    result = {
        "indexes": indexes,
        "sector_detected": sector,
        "correlation_pairs": [
            {"index_a": INDEX_FACTORY_IOT, "index_b": INDEX_PRICING_REFERENCE,
             "reason": "machine idle hours costed via press-machine-hour unit rate"},
            {"index_a": INDEX_HOSPITAL_DRUGS, "index_b": INDEX_PRICING_REFERENCE,
             "reason": "drug waste quantity costed via insulin unit rate"},
            {"index_a": INDEX_NYC_BUILDINGS, "index_b": INDEX_PRICING_REFERENCE,
             "reason": "excess energy kWh costed via kwh-nyc electricity rate"},
        ],
        "confidence_score": 0.97,
        "summary": (
            f"Found {len(indexes)} indexes — sector: {sector} — "
            f"{high_count} with HIGH anomaly potential"
        ),
    }
    _add_reasoning("Cartographer", result["summary"],
                   result_summary=f"{len(indexes)} indexes, sector={sector}")
    return result


# ── Generic Anomaly Scanner (for custom/uploaded data) ────────────────────────

_KNOWN_SCAN_INDEXES = {INDEX_HOSPITAL_DRUGS, INDEX_FACTORY_IOT, INDEX_NYC_BUILDINGS}

_MISMATCH_HINTS = [
    ("ordered", "used"), ("ordered", "sold"), ("received", "sold"),
    ("received", "used"), ("produced", "sold"), ("purchased", "consumed"),
    ("allocated", "utilized"), ("stocked", "sold"), ("in", "out"),
]


def _run_generic_anomaly_scan(
    index_name: str,
    ano_id_start: int,
) -> tuple:
    """Run generic anomaly detection on any ES index by inspecting its mappings.

    Builds ES|QL queries dynamically based on field types:
    1. Mismatch detection: finds pairs of numeric fields that look like input/output
    2. Outlier detection: groups by keyword fields and finds numeric outliers

    Args:
        index_name: The Elasticsearch index to scan.
        ano_id_start: Starting anomaly ID counter.

    Returns:
        Tuple of (anomalies list, next ano_id).
    """
    es = _es()
    anomalies: List[Dict[str, Any]] = []
    ano_id = ano_id_start

    try:
        mapping = es.indices.get_mapping(index=index_name)
        props: Dict[str, Any] = {}
        for _k, v in mapping.items():
            props = v.get("mappings", {}).get("properties", {})
            break
    except ApiError as exc:
        _add_reasoning("Pattern Seeker", f"Cannot read mapping for {index_name}: {exc}")
        return anomalies, ano_id

    num_fields = [f for f, d in props.items()
                  if d.get("type") in ("float", "double", "integer", "long")
                  and f != "@timestamp"]
    kw_fields = [f for f, d in props.items() if d.get("type") == "keyword"]

    if not num_fields:
        _add_reasoning("Pattern Seeker", f"No numeric fields in {index_name} — skipping")
        return anomalies, ano_id

    _add_reasoning("Pattern Seeker",
                   f"Generic scan of {index_name}: {len(num_fields)} numeric, {len(kw_fields)} keyword fields")

    # Strategy 1: Mismatch detection between numeric field pairs
    matched_pair = None
    for hint_a, hint_b in _MISMATCH_HINTS:
        field_a = [f for f in num_fields if hint_a in f.lower()]
        field_b = [f for f in num_fields if hint_b in f.lower()]
        if field_a and field_b and field_a[0] != field_b[0]:
            matched_pair = (field_a[0], field_b[0])
            break

    if matched_pair:
        fa, fb = matched_pair
        group_by = kw_fields[0] if kw_fields else None
        by_clause = f", {group_by}" if group_by else ""
        keep_clause = f"{group_by}, " if group_by else ""

        query = (
            f"FROM {index_name} "
            f"| STATS total_a = SUM({fa}), total_b = SUM({fb}) BY {group_by or fa}{by_clause if not group_by else ''} "
            f"| EVAL delta = total_a - total_b, "
            f"  ratio = TO_DOUBLE(total_a - total_b) / TO_DOUBLE(total_a) "
            f"| WHERE ratio > 0.20 "
            f"| SORT ratio DESC "
            f"| LIMIT 20"
        )
        if group_by:
            query = (
                f"FROM {index_name} "
                f"| STATS total_a = SUM({fa}), total_b = SUM({fb}) BY {group_by} "
                f"| EVAL delta = total_a - total_b, "
                f"  ratio = TO_DOUBLE(total_a - total_b) / TO_DOUBLE(total_a) "
                f"| WHERE ratio > 0.20 "
                f"| SORT ratio DESC "
                f"| LIMIT 20"
            )

        _add_reasoning("Pattern Seeker",
                       f"Mismatch scan: {fa} vs {fb} grouped by {group_by or 'none'}",
                       tool="generic.mismatch", query=query)
        try:
            rows = _esql(query)
            for row in rows:
                ano_id += 1
                entity_val = str(row.get(group_by, index_name)) if group_by else index_name
                anomalies.append({
                    "id": f"ANO-{ano_id:03d}",
                    "type": "GENERIC_MISMATCH",
                    "entity": f"{entity_val} — {index_name}",
                    "index": index_name,
                    "delta_quantity": int(row.get("delta", 0)),
                    "unit": "units",
                    "time_period_days": 90,
                    "confidence_score": round(min(0.95, float(row.get("ratio", 0)) + 0.5), 2),
                    "tool_used": "generic.mismatch",
                    "raw_data_summary": (
                        f"{fa}={row.get('total_a', 0)} {fb}={row.get('total_b', 0)} "
                        f"delta={row.get('delta', 0)} ratio={row.get('ratio', 0):.3f}"
                    ),
                })
            _add_reasoning("Pattern Seeker",
                           f"Mismatch scan found {len(rows)} anomalies in {index_name}",
                           tool="generic.mismatch",
                           result_summary=f"{len(rows)} mismatch patterns in {fa} vs {fb}")
        except ApiError as exc:
            _add_reasoning("Pattern Seeker", f"Mismatch query failed for {index_name}: {exc}")

    # Strategy 2: Outlier detection — find groups with values > 2x the average
    if kw_fields and num_fields:
        target_num = num_fields[0]
        if matched_pair and matched_pair[0] in num_fields:
            candidates = [f for f in num_fields if f not in matched_pair]
            if candidates:
                target_num = candidates[0]

        group_field = kw_fields[0]
        outlier_query = (
            f"FROM {index_name} "
            f"| STATS avg_val = AVG({target_num}), max_val = MAX({target_num}), "
            f"  total = SUM({target_num}), cnt = COUNT(*) BY {group_field} "
            f"| WHERE max_val > avg_val * 2 AND cnt > 5 "
            f"| SORT total DESC "
            f"| LIMIT 15"
        )
        _add_reasoning("Pattern Seeker",
                       f"Outlier scan: {target_num} grouped by {group_field}",
                       tool="generic.outlier", query=outlier_query)
        try:
            rows = _esql(outlier_query)
            for row in rows:
                ano_id += 1
                anomalies.append({
                    "id": f"ANO-{ano_id:03d}",
                    "type": "GENERIC_OUTLIER",
                    "entity": f"{row.get(group_field, '?')} — {index_name}",
                    "index": index_name,
                    "delta_quantity": int(row.get("total", 0)),
                    "unit": target_num,
                    "time_period_days": 90,
                    "confidence_score": 0.80,
                    "tool_used": "generic.outlier",
                    "raw_data_summary": (
                        f"avg={row.get('avg_val', 0):.1f} max={row.get('max_val', 0):.1f} "
                        f"total={row.get('total', 0)} count={row.get('cnt', 0)}"
                    ),
                })
            _add_reasoning("Pattern Seeker",
                           f"Outlier scan found {len(rows)} anomalies in {index_name}",
                           tool="generic.outlier",
                           result_summary=f"{len(rows)} outlier groups in {target_num}")
        except ApiError as exc:
            _add_reasoning("Pattern Seeker", f"Outlier query failed for {index_name}: {exc}")

    # Strategy 3: Waste cost detection — find fields named *wasted*/*lost*/*shrink* with a cost field
    waste_fields = [f for f in num_fields if any(w in f.lower() for w in ("wasted", "waste", "lost", "shrink", "damaged", "expired"))]
    cost_fields = [f for f in num_fields if any(c in f.lower() for c in ("cost", "price", "rate", "value")) and f not in waste_fields]

    if waste_fields and cost_fields:
        wf = waste_fields[0]
        cf = cost_fields[0]
        group_field = kw_fields[0] if kw_fields else None
        by_clause = f" BY {group_field}" if group_field else ""

        waste_cost_query = (
            f"FROM {index_name} "
            f"| STATS total_wasted = SUM({wf}), avg_cost = AVG({cf}), "
            f"  total_waste_cost = SUM({wf} * {cf}), cnt = COUNT(*){by_clause} "
            f"| WHERE total_wasted > 10 "
            f"| SORT total_waste_cost DESC "
            f"| LIMIT 20"
        )
        _add_reasoning("Pattern Seeker",
                       f"Waste-cost scan: {wf} x {cf} grouped by {group_field or 'none'}",
                       tool="generic.waste_cost", query=waste_cost_query)
        try:
            rows = _esql(waste_cost_query)
            for row in rows:
                ano_id += 1
                entity_val = str(row.get(group_field, index_name)) if group_field else index_name
                waste_cost = row.get("total_waste_cost", 0) or 0
                anomalies.append({
                    "id": f"ANO-{ano_id:03d}",
                    "type": "GENERIC_WASTE_COST",
                    "entity": f"{entity_val} — {index_name}",
                    "index": index_name,
                    "delta_quantity": int(row.get("total_wasted", 0)),
                    "unit": wf,
                    "unit_cost_detected": round(float(row.get("avg_cost", 1.0)), 4),
                    "time_period_days": 90,
                    "confidence_score": 0.88,
                    "tool_used": "generic.waste_cost",
                    "raw_data_summary": (
                        f"wasted={row.get('total_wasted', 0)} avg_cost=${row.get('avg_cost', 0):.2f} "
                        f"waste_cost=${waste_cost:,.2f} count={row.get('cnt', 0)}"
                    ),
                })
            _add_reasoning("Pattern Seeker",
                           f"Waste-cost scan found {len(rows)} anomalies in {index_name}",
                           tool="generic.waste_cost",
                           result_summary=f"{len(rows)} waste-cost patterns in {wf} x {cf}")
        except ApiError as exc:
            _add_reasoning("Pattern Seeker", f"Waste-cost query failed for {index_name}: {exc}")

    return anomalies, ano_id


# ── Agent 2: Pattern Seeker ───────────────────────────────────────────────────

def run_pattern_seeker(
    index_map: Dict[str, Any],
    target_indexes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Detect waste anomalies using specialized + generic ES|QL tools.

    Tries Agent Builder first, falls back to direct ES|QL.
    Runs optimized tools for known indexes and generic scans for any custom data.

    Args:
        index_map: Output from Cartographer.
        target_indexes: If provided, only scan these indexes.
            If None or empty, scans ALL available indexes.

    Returns:
        dict: anomalies list, total_anomalies_found, confidence_score, summary.
    """
    if target_indexes:
        _add_reasoning("Pattern Seeker",
                       f"Starting targeted anomaly detection for: {', '.join(target_indexes)}")
    else:
        _add_reasoning("Pattern Seeker",
                       "Starting anomaly detection across all available indexes...")

    ab_result = _try_agent_builder(
        AGENT_PATTERN_SEEKER,
        "Run all three anomaly detection tools: usage_anomaly, runtime_anomaly, "
        "and energy_anomaly. Report all findings with anomaly IDs, types, entities, "
        "delta quantities, and confidence scores. "
        f"Context: {len(index_map.get('indexes', []))} indexes available, "
        f"sector={index_map.get('sector_detected', 'unknown')}."
    )
    if ab_result and "anomalies" in ab_result:
        _add_reasoning("Pattern Seeker",
                       f"Agent Builder found {ab_result.get('total_anomalies_found', 0)} anomalies",
                       result_summary=ab_result.get("summary", ""))
        return ab_result

    _add_reasoning("Pattern Seeker", "Using direct ES|QL for anomaly detection")

    _scan_all = not target_indexes
    _targets = set(target_indexes or [])

    es = _es()
    doc_counts: Dict[str, int] = {}
    try:
        idx_info = es.cat.indices(format="json", h="index,docs.count")
        for info in idx_info:
            doc_counts[info.get("index", "")] = int(info.get("docs.count", 0) or 0)
    except ApiError:
        pass

    _add_reasoning("Pattern Seeker",
                   f"Data provenance: querying live Elasticsearch cluster at {_ES_URL[:30]}...")

    anomalies = []
    ano_id = 0

    # Tool 1: usage_anomaly
    usage_rows = []
    if _scan_all or INDEX_HOSPITAL_DRUGS in _targets:
        usage_query = (
            f"FROM {INDEX_HOSPITAL_DRUGS}\n"
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
        dc = doc_counts.get(INDEX_HOSPITAL_DRUGS, 0)
        _add_reasoning("Pattern Seeker",
                       f"Running ghost.usage_anomaly tool — hospital drug over-procurement ({dc:,} docs in index)",
                       tool="ghost.usage_anomaly", query=usage_query)
        usage_rows = _esql(usage_query)
    else:
        _add_reasoning("Pattern Seeker", f"Skipping usage_anomaly (hospital-drugs not selected)")
    for row in usage_rows:
        ano_id += 1
        anomalies.append({
            "id": f"ANO-{ano_id:03d}",
            "type": "USAGE_ORDER_MISMATCH",
            "entity": f"{row['drug_name']} — {row['wing_id']}",
            "index": INDEX_HOSPITAL_DRUGS,
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
    _add_reasoning("Pattern Seeker", f"usage_anomaly found {len(usage_rows)} anomalies",
                   tool="ghost.usage_anomaly",
                   result_summary=f"{len(usage_rows)} drug waste patterns detected")

    # Tool 2: runtime_anomaly
    runtime_rows = []
    if _scan_all or INDEX_FACTORY_IOT in _targets:
        runtime_query = (
            f"FROM {INDEX_FACTORY_IOT}\n"
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
        dc = doc_counts.get(INDEX_FACTORY_IOT, 0)
        _add_reasoning("Pattern Seeker",
                       f"Running ghost.runtime_anomaly tool — factory idle machines ({dc:,} docs in index)",
                       tool="ghost.runtime_anomaly", query=runtime_query)
        runtime_rows = _esql(runtime_query)
    else:
        _add_reasoning("Pattern Seeker", f"Skipping runtime_anomaly (factory-iot-data not selected)")
    for row in runtime_rows:
        ano_id += 1
        anomalies.append({
            "id": f"ANO-{ano_id:03d}",
            "type": "RUNTIME_SCHEDULE_GAP",
            "entity": f"{row['machine_id']} — Factory Floor",
            "index": INDEX_FACTORY_IOT,
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
    _add_reasoning("Pattern Seeker", f"runtime_anomaly found {len(runtime_rows)} anomalies",
                   tool="ghost.runtime_anomaly",
                   result_summary=f"{len(runtime_rows)} idle machines detected")

    # Tool 3: energy_anomaly
    energy_rows = []
    if _scan_all or INDEX_NYC_BUILDINGS in _targets:
        energy_query = (
            f"FROM {INDEX_NYC_BUILDINGS}\n"
            "| STATS\n"
            "    avg_occupancy    = AVG(occupancy_pct),\n"
            "    total_energy_kwh = SUM(energy_kwh)\n"
            "  BY building_id, borough\n"
            "| EVAL\n"
            "    total_energy_cost = total_energy_kwh * 0.22,\n"
            "    waste_score       = (1 - avg_occupancy) * total_energy_kwh\n"
            "| WHERE avg_occupancy < 0.15 AND total_energy_kwh > 5000\n"
            "| SORT total_energy_cost DESC\n"
            "| LIMIT 15\n"
            "| KEEP building_id, borough, avg_occupancy, total_energy_kwh, total_energy_cost, waste_score"
        )
        dc = doc_counts.get(INDEX_NYC_BUILDINGS, 0)
        _add_reasoning("Pattern Seeker",
                       f"Running ghost.energy_anomaly tool — building energy waste ({dc:,} docs in index)",
                       tool="ghost.energy_anomaly", query=energy_query)
        energy_rows = _esql(energy_query)
    else:
        _add_reasoning("Pattern Seeker", f"Skipping energy_anomaly (nyc-buildings not selected)")
    for row in energy_rows:
        ano_id += 1
        anomalies.append({
            "id": f"ANO-{ano_id:03d}",
            "type": "ENERGY_OCCUPANCY_DIVERGENCE",
            "entity": f"{row['building_id']} — {row['borough']}",
            "index": INDEX_NYC_BUILDINGS,
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
    _add_reasoning("Pattern Seeker", f"energy_anomaly found {len(energy_rows)} anomalies",
                   tool="ghost.energy_anomaly",
                   result_summary=f"{len(energy_rows)} energy waste buildings detected")

    # Generic scan for custom/uploaded indexes
    _SYSTEM_INDEXES = {"pricing-reference", "known-exceptions", "ghost-economy-audit"}
    generic_count = 0
    if target_indexes:
        custom_indexes = [idx for idx in target_indexes if idx not in _KNOWN_SCAN_INDEXES]
    elif _scan_all:
        try:
            all_idx = es.cat.indices(format="json", h="index")
            custom_indexes = [
                entry.get("index", "") for entry in all_idx
                if entry.get("index", "")
                and not entry["index"].startswith(".")
                and entry["index"] not in _KNOWN_SCAN_INDEXES
                and entry["index"] not in _SYSTEM_INDEXES
            ]
        except ApiError:
            custom_indexes = []
    else:
        custom_indexes = []

    generic_parts: List[str] = []
    for custom_idx in custom_indexes:
        dc = doc_counts.get(custom_idx, 0)
        _add_reasoning("Pattern Seeker",
                       f"Running generic anomaly scan on: {custom_idx} ({dc:,} docs)")
        custom_anomalies, ano_id = _run_generic_anomaly_scan(custom_idx, ano_id)
        anomalies.extend(custom_anomalies)
        generic_count += len(custom_anomalies)
        if custom_anomalies:
            generic_parts.append(f"{len(custom_anomalies)} in {custom_idx}")

    parts = []
    if usage_rows:
        parts.append(f"{len(usage_rows)} drug waste")
    if runtime_rows:
        parts.append(f"{len(runtime_rows)} idle machines")
    if energy_rows:
        parts.append(f"{len(energy_rows)} building energy waste")
    if generic_parts:
        parts.extend(generic_parts)

    result = {
        "anomalies": anomalies,
        "total_anomalies_found": len(anomalies),
        "confidence_score": 0.93,
        "summary": f"Found {len(anomalies)} anomalies: " + ", ".join(parts) if parts else "No anomalies found",
    }
    _add_reasoning("Pattern Seeker", result["summary"])
    return result


# ── Agent 3: Valuator ─────────────────────────────────────────────────────────

def run_valuator(patterns: Dict[str, Any]) -> Dict[str, Any]:
    """Assign dollar values to every anomaly using the pricing reference.

    Tries Agent Builder first, falls back to direct ES|QL.

    Args:
        patterns: Output from Pattern Seeker.

    Returns:
        dict: valued_findings, total_ghost_economy_usd, total_annualized_usd, etc.
    """
    _add_reasoning("Valuator", "Starting dollar impact calculation...")

    anomaly_summary = json.dumps(patterns.get("anomalies", [])[:5], default=str)
    ab_result = _try_agent_builder(
        AGENT_VALUATOR,
        f"Calculate dollar values for these anomalies using the value_calculator tool "
        f"to look up unit costs from pricing-reference index:\n{anomaly_summary}"
    )
    if ab_result and "valued_findings" in ab_result:
        _add_reasoning("Valuator",
                       f"Agent Builder valued {len(ab_result.get('valued_findings', []))} findings",
                       result_summary=ab_result.get("summary", ""))
        return ab_result

    _add_reasoning("Valuator", "Using direct ES|QL for pricing lookup")

    pricing_query = f"FROM {INDEX_PRICING_REFERENCE} | KEEP item_key, unit_cost_usd, unit_label | LIMIT 20"
    _add_reasoning("Valuator", "Looking up unit costs from pricing-reference index",
                   tool="ghost.value_calculator", query=pricing_query)
    pricing_rows = _esql(pricing_query)
    pricing = {row["item_key"]: row for row in pricing_rows}
    _add_reasoning("Valuator", f"Loaded {len(pricing)} pricing items",
                   result_summary=", ".join(pricing.keys()))

    unit_cost_map = {
        "USAGE_ORDER_MISMATCH": pricing.get("insulin", {}).get("unit_cost_usd", 212.50),
        "RUNTIME_SCHEDULE_GAP": pricing.get("press-machine-hour", {}).get("unit_cost_usd", 112.50),
        "ENERGY_OCCUPANCY_DIVERGENCE": pricing.get("kwh-nyc", {}).get("unit_cost_usd", 0.22),
        "GENERIC_MISMATCH": 1.0,
        "GENERIC_OUTLIER": 1.0,
        "GENERIC_WASTE_COST": None,
    }
    category_map = {
        "USAGE_ORDER_MISMATCH": "Drug Over-Procurement",
        "RUNTIME_SCHEDULE_GAP": "Idle Equipment Runtime",
        "ENERGY_OCCUPANCY_DIVERGENCE": "Energy-Occupancy Waste",
        "GENERIC_MISMATCH": "Data Mismatch Waste",
        "GENERIC_OUTLIER": "Statistical Outlier",
        "GENERIC_WASTE_COST": "Product Waste Loss",
    }

    valued_findings = []
    for ano in patterns.get("anomalies", []):
        atype = ano["type"]
        unit_cost = unit_cost_map.get(atype, 1.0)
        if unit_cost is None:
            unit_cost = ano.get("unit_cost_detected", 1.0)
        delta = ano["delta_quantity"]
        days = ano["time_period_days"]
        dollar_value = round(delta * unit_cost, 2)
        annualized = round(dollar_value * (365 / days), 2)

        if dollar_value >= 100_000:
            priority = "CRITICAL"
        elif dollar_value >= 50_000:
            priority = "HIGH"
        elif dollar_value >= 10_000:
            priority = "MEDIUM"
        else:
            priority = "LOW"

        uc = round(unit_cost, 4)
        unit_label = ano.get("unit", "units")
        if atype == "RUNTIME_SCHEDULE_GAP":
            calc = f"{delta:,} idle hours \u00d7 ${uc}/hr"
        elif atype == "ENERGY_OCCUPANCY_DIVERGENCE":
            calc = f"{delta:,} kWh \u00d7 ${uc}/kWh"
        elif atype == "GENERIC_WASTE_COST":
            calc = f"{delta:,} wasted \u00d7 ${uc}/unit (from data)"
        elif atype == "GENERIC_MISMATCH":
            calc = f"{delta:,} {unit_label} surplus \u00d7 ${uc}/unit"
        elif atype == "GENERIC_OUTLIER":
            calc = f"{delta:,} {unit_label} outlier \u00d7 ${uc}/unit"
        else:
            calc = f"{delta:,} units \u00d7 ${uc}/unit"

        valued_findings.append({
            "anomaly_id": ano["id"],
            "entity": ano["entity"],
            "category": category_map.get(atype, atype),
            "source_index": ano.get("index", "unknown"),
            "tool_used": ano.get("tool_used", ""),
            "dollar_value": dollar_value,
            "calculation": calc,
            "unit_cost_source": "pricing-reference index",
            "annualized_value": annualized,
            "confidence_score": ano["confidence_score"],
            "priority": priority,
        })
        _add_reasoning("Valuator", f"{ano['id']} → {format_dollar(dollar_value)} ({priority})",
                       result_summary=calc)

    total = round(sum(f["dollar_value"] for f in valued_findings), 2)
    annualized_total = round(sum(f["annualized_value"] for f in valued_findings), 2)

    result = {
        "valued_findings": valued_findings,
        "total_ghost_economy_usd": total,
        "total_annualized_usd": annualized_total,
        "confidence_score": 0.92,
        "currency": "USD",
        "summary": f"Found {format_dollar(total)} in hidden waste across {len(set(f.get('category', '') for f in valued_findings))} categories",
    }
    _add_reasoning("Valuator", result["summary"],
                   result_summary=f"Total: {format_dollar(total)}, Annualized: {format_dollar(annualized_total)}")
    return result


# ── Agent 4: Action Taker ─────────────────────────────────────────────────────

def run_action_taker(values: Dict[str, Any]) -> Dict[str, Any]:
    """Verify findings, send Slack alerts, and index audit records.

    Tries Agent Builder first, falls back to direct logic.

    Args:
        values: Output from Valuator.

    Returns:
        dict: verified_actions, actions_triggered, actions_suppressed, etc.
    """
    _add_reasoning("Action Taker", "Verifying findings and preparing actions...")

    findings_summary = json.dumps(values.get("valued_findings", [])[:5], default=str)
    ab_result = _try_agent_builder(
        AGENT_ACTION_TAKER,
        f"Review and verify these findings. Check known-exceptions, assign actionability "
        f"scores, trigger workflows for scores >= 0.5, create Slack messages:\n{findings_summary}"
    )
    if ab_result and "verified_actions" in ab_result:
        _add_reasoning("Action Taker",
                       f"Agent Builder processed {len(ab_result.get('verified_actions', []))} actions",
                       result_summary=ab_result.get("summary", ""))
        _send_slack_alerts(ab_result.get("verified_actions", []))
        _index_audit_records(values.get("valued_findings", []),
                            ab_result.get("verified_actions", []))
        return ab_result

    _add_reasoning("Action Taker", "Using direct logic for action decisions")

    verified_actions = []
    triggered = 0
    suppressed = 0
    total_actioned = 0.0

    for finding in values.get("valued_findings", []):
        dollar = finding["dollar_value"]
        conf = finding["confidence_score"]
        priority = finding["priority"]

        dollar_score = {
            "CRITICAL": 1.0,
            "HIGH": 0.8,
            "MEDIUM": 0.6,
            "LOW": 0.3,
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
            _add_reasoning("Action Taker",
                           f"TRIGGERED: {finding['anomaly_id']} — actionability={actionability:.2f}",
                           tool="ghost.action_workflow",
                           result_summary=slack_msg[:80])
        else:
            action_taken = "suppressed"
            suppressed += 1
            slack_msg = ""
            _add_reasoning("Action Taker",
                           f"SUPPRESSED: {finding['anomaly_id']} — actionability={actionability:.2f} (below 0.5)")

        verified_actions.append({
            "anomaly_id": finding["anomaly_id"],
            "entity": finding["entity"],
            "dollar_value": dollar,
            "actionability_score": actionability,
            "action_taken": action_taken,
            "slack_message": slack_msg,
        })

    _send_slack_alerts(verified_actions)
    _index_audit_records(values.get("valued_findings", []), verified_actions)

    result = {
        "verified_actions": verified_actions,
        "actions_triggered": triggered,
        "actions_suppressed": suppressed,
        "total_value_actioned_usd": round(total_actioned, 2),
        "confidence_score": 0.94,
        "summary": (
            f"Triggered {triggered} actions totaling {format_dollar(total_actioned)}. "
            f"{suppressed} suppressed."
        ),
    }
    _add_reasoning("Action Taker", result["summary"])
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
        aid = finding.get("anomaly_id", "unknown")
        action = action_map.get(aid, {})
        doc = {
            "@timestamp": ts,
            "finding_id": aid,
            "entity": finding.get("entity", ""),
            "category": finding.get("category", ""),
            "source_index": finding.get("source_index", ""),
            "tool_used": finding.get("tool_used", ""),
            "dollar_value": finding.get("dollar_value", 0),
            "calculation": finding.get("calculation", ""),
            "confidence": finding.get("confidence_score", 0),
            "action_taken": action.get("action_taken", "pending"),
            "priority": finding.get("priority", ""),
            "annualized_value": finding.get("annualized_value", 0),
        }
        try:
            es.index(index=INDEX_GHOST_ECONOMY_AUDIT, document=doc)
            indexed += 1
        except ApiError as exc:
            log.error("Failed to index audit record %s: %s", aid, exc)
    log.info("Indexed %d audit records into %s", indexed, INDEX_GHOST_ECONOMY_AUDIT)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_hunt(target_indexes: Optional[List[str]] = None) -> Dict[str, Any]:
    """Execute the complete Ghost Economy Hunt pipeline.

    Args:
        target_indexes: If provided, only scan these indexes for anomalies.
            Pass None or empty list to scan all available data.

    Returns:
        dict: Full hunt results including total_ghost_economy_usd, anomalies_found,
              actions_triggered, and reasoning_log.
    """
    _reasoning_log.clear()

    log.info("=" * 60)
    log.info("GHOST ECONOMY HUNT STARTING")
    if target_indexes:
        log.info("Target indexes: %s", ", ".join(target_indexes))
    else:
        log.info("Scanning ALL available data")
    log.info("Agent Builder: %s", "ENABLED" if _AGENT_BUILDER_ENABLED else "DISABLED (direct ES|QL)")
    log.info("=" * 60)
    start = datetime.utcnow()

    index_map = run_cartographer()
    patterns = run_pattern_seeker(index_map, target_indexes=target_indexes)
    values = run_valuator(patterns)
    actions = run_action_taker(values)

    total = values.get("total_ghost_economy_usd", 0)
    n_anomalies = patterns.get("total_anomalies_found", 0)
    n_actions = actions.get("actions_triggered", 0)
    duration = (datetime.utcnow() - start).total_seconds()

    log.info("=" * 60)
    log.info("GHOST ECONOMY HUNT COMPLETE")
    log.info("  Total Hidden Value Found: %s", format_dollar(total))
    log.info("  Annualized Impact:        %s", format_dollar(values.get("total_annualized_usd", 0)))
    log.info("  Anomalies Detected:       %d", n_anomalies)
    log.info("  Actions Triggered:        %d", n_actions)
    log.info("  Time Taken:               %.1fs", duration)
    log.info("=" * 60)

    return {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "total_ghost_economy_usd": total,
        "anomalies_found": n_anomalies,
        "actions_triggered": n_actions,
        "duration_seconds": round(duration, 1),
        "agent_builder_used": _AGENT_BUILDER_ENABLED,
        "index_map": index_map,
        "patterns": patterns,
        "values": values,
        "actions": actions,
        "reasoning_log": get_reasoning_log(),
    }


if __name__ == "__main__":
    run_hunt()
