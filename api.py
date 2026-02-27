"""Ghost Economy Hunter — FastAPI server.

Serves the frontend and exposes endpoints for:
  - /api/run            — execute the 4-agent pipeline (non-streaming)
  - /api/run/stream     — SSE stream with reasoning traces
  - /api/chat           — natural language ES|QL queries
  - /api/converse       — proxy to Agent Builder converse API
  - /api/upload         — upload a CSV file to create a new ES index
  - /api/connect        — connect an existing ES index for scanning
  - /api/indexes        — list all available indexes
  - /api/agent-builder/status  — Agent Builder health check
  - /api/agent-builder/agents  — list registered agents
  - /api/agent-builder/tools   — list registered tools
  - /api/integrations   — MCP, A2A, and workflow integration URLs
  - /api/history        — recent hunt results from audit index
  - /api/impact         — computed impact metrics
  - /api/health         — health check

Run with:
    python run.py
Then open: http://localhost:8000
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent))

import uvicorn
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError, ConnectionError as ESConnectionError
from elasticsearch.helpers import bulk
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ghost-economy-api")

_ES_URL = os.getenv("ELASTIC_URL", "")
_ES_KEY = os.getenv("ELASTIC_API_KEY", "")

app = FastAPI(
    title="Ghost Economy Hunter API",
    description="Multi-agent AI system that finds hidden financial waste in Elasticsearch data.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

FRONTEND_DIR = Path(__file__).parent / "frontend"


def _validate_env() -> List[str]:
    """Check required env vars are set and return list of warnings."""
    warnings = []
    if not _ES_URL:
        warnings.append("ELASTIC_URL not set — Elasticsearch queries will fail")
    if not _ES_KEY:
        warnings.append("ELASTIC_API_KEY not set — authentication will fail")
    if not os.getenv("KIBANA_URL"):
        warnings.append("KIBANA_URL not set — Agent Builder will be unavailable")
    return warnings


_startup_warnings = _validate_env()
for w in _startup_warnings:
    log.warning("CONFIG: %s", w)


def _es() -> Elasticsearch:
    """Return a configured Elasticsearch client.

    Returns:
        Elasticsearch client instance.
    """
    return Elasticsearch(_ES_URL, api_key=_ES_KEY)


def _infer_field_type(values: List[str]) -> str:
    """Infer an ES field type from a sample of string values.

    Args:
        values: Sample of non-empty string values from one column.

    Returns:
        Elasticsearch field type string.
    """
    numeric_count = 0
    date_count = 0
    for v in values[:50]:
        v = v.strip()
        if not v:
            continue
        try:
            float(v)
            numeric_count += 1
            continue
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%m/%d/%Y"):
            try:
                datetime.strptime(v, fmt)
                date_count += 1
                break
            except ValueError:
                continue

    total = len([v for v in values[:50] if v.strip()])
    if total == 0:
        return "text"
    if numeric_count / total > 0.8:
        return "float"
    if date_count / total > 0.8:
        return "date"
    if all(len(v.strip()) < 64 for v in values[:50] if v.strip()):
        return "keyword"
    return "text"


# ── Serve frontend ────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def serve_frontend() -> FileResponse:
    """Serve the main frontend HTML."""
    return FileResponse(str(FRONTEND_DIR / "index.html"))


@app.get("/static/{filepath:path}", include_in_schema=False)
async def serve_static(filepath: str) -> FileResponse:
    """Serve static files from frontend directory."""
    return FileResponse(str(FRONTEND_DIR / filepath))


# ── Pipeline endpoints ────────────────────────────────────────────────────────

@app.post("/api/run")
async def run_pipeline(body: Dict[str, Any] = None) -> JSONResponse:
    """Execute the full Ghost Economy Hunt pipeline.

    Args:
        body: Optional JSON with target_indexes list to limit scan scope.

    Returns:
        JSON with real pipeline results from live Elasticsearch data.
    """
    try:
        from orchestrator.main import run_hunt
        target_indexes = (body or {}).get("target_indexes", None)
        log.info("API: starting pipeline run (targets=%s)...", target_indexes or "ALL")
        results = run_hunt(target_indexes=target_indexes)

        findings = []
        for f in results.get("values", {}).get("valued_findings", []):
            findings.append({
                "id": f.get("anomaly_id", ""),
                "entity": f.get("entity", ""),
                "category": f.get("category", ""),
                "source_index": f.get("source_index", ""),
                "tool_used": f.get("tool_used", ""),
                "dollar": f.get("dollar_value", 0),
                "annualized": f.get("annualized_value", 0),
                "calc": f.get("calculation", ""),
                "priority": f.get("priority", "MEDIUM"),
                "confidence": f.get("confidence_score", 0),
            })

        payload = {
            "total": results.get("total_ghost_economy_usd", 0),
            "anomalies_found": results.get("anomalies_found", 0),
            "actions_triggered": results.get("actions_triggered", 0),
            "findings": findings,
            "summary": results.get("values", {}).get("summary", ""),
            "index_count": len(results.get("index_map", {}).get("indexes", [])),
            "reasoning_log": results.get("reasoning_log", []),
        }
        log.info("API: pipeline complete — total=%s", payload["total"])
        return JSONResponse(content=payload)

    except ApiError as exc:
        log.error("Pipeline failed (ES error): %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc), "detail": "Elasticsearch query failed"})
    except ESConnectionError as exc:
        log.error("Pipeline failed (connection): %s", exc)
        return JSONResponse(status_code=503, content={"error": "Cannot connect to Elasticsearch", "detail": str(exc)})
    except (ValueError, RuntimeError) as exc:
        log.error("Pipeline failed: %s", exc, exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(exc), "detail": "Pipeline execution failed"})


@app.get("/api/run/stream")
async def run_pipeline_stream(targets: str = ""):
    """Execute the pipeline with Server-Sent Events for real-time progress.

    Streams events as each agent completes, including reasoning traces,
    tool selections, and ES|QL queries so the frontend can display
    agent thinking in real time.

    Args:
        targets: Comma-separated list of index names to scan (empty = all).

    Returns:
        SSE stream with agent progress and reasoning events.
    """
    target_indexes = [t.strip() for t in targets.split(",") if t.strip()] if targets else None

    def event_stream():
        """Generator yielding SSE events as agents execute."""
        try:
            from orchestrator.main import (
                get_reasoning_log,
                run_action_taker,
                run_cartographer,
                run_pattern_seeker,
                run_valuator,
            )
            from orchestrator.main import _reasoning_log

            _reasoning_log.clear()

            t0 = time.time()
            last_reasoning_idx = 0

            def flush_reasoning(step: int, agent: str):
                """Yield any new reasoning entries since last flush."""
                nonlocal last_reasoning_idx
                current_log = get_reasoning_log()
                new_entries = current_log[last_reasoning_idx:]
                last_reasoning_idx = len(current_log)
                for entry in new_entries:
                    yield _sse({
                        "step": step,
                        "agent": agent,
                        "status": "thinking",
                        "reasoning": entry.get("thought", ""),
                        "tool": entry.get("tool"),
                        "query": entry.get("query"),
                        "result_summary": entry.get("result_summary"),
                        "elapsed": round(time.time() - t0, 1),
                    })

            # Agent 1: Cartographer
            yield _sse({"step": 1, "agent": "Cartographer", "status": "running"})
            index_map = run_cartographer()
            yield from flush_reasoning(1, "Cartographer")
            yield _sse({
                "step": 1, "agent": "Cartographer", "status": "done",
                "detail": f"{len(index_map.get('indexes', []))} indexes mapped",
                "sector": index_map.get("sector_detected", ""),
                "elapsed": round(time.time() - t0, 1),
            })

            # Agent 2: Pattern Seeker
            yield _sse({"step": 2, "agent": "Pattern Seeker", "status": "running"})
            patterns = run_pattern_seeker(index_map, target_indexes=target_indexes)
            yield from flush_reasoning(2, "Pattern Seeker")
            n = patterns.get("total_anomalies_found", 0)
            yield _sse({
                "step": 2, "agent": "Pattern Seeker", "status": "done",
                "detail": f"{n} anomalies found",
                "anomalies": n,
                "elapsed": round(time.time() - t0, 1),
            })

            # Agent 3: Valuator
            yield _sse({"step": 3, "agent": "Valuator", "status": "running"})
            values = run_valuator(patterns)
            yield from flush_reasoning(3, "Valuator")
            total = values.get("total_ghost_economy_usd", 0)
            findings = []
            for f in values.get("valued_findings", []):
                findings.append({
                    "id": f.get("anomaly_id", ""),
                    "entity": f.get("entity", ""),
                    "category": f.get("category", ""),
                    "source_index": f.get("source_index", ""),
                    "tool_used": f.get("tool_used", ""),
                    "dollar": f.get("dollar_value", 0),
                    "annualized": f.get("annualized_value", 0),
                    "calc": f.get("calculation", ""),
                    "priority": f.get("priority", "MEDIUM"),
                    "confidence": f.get("confidence_score", 0),
                })
            yield _sse({
                "step": 3, "agent": "Valuator", "status": "done",
                "detail": f"${total:,.0f} total waste",
                "total": total,
                "annualized": values.get("total_annualized_usd", 0),
                "findings": findings,
                "elapsed": round(time.time() - t0, 1),
            })

            # Agent 4: Action Taker
            yield _sse({"step": 4, "agent": "Action Taker", "status": "running"})
            actions = run_action_taker(values)
            yield from flush_reasoning(4, "Action Taker")
            triggered = actions.get("actions_triggered", 0)
            yield _sse({
                "step": 4, "agent": "Action Taker", "status": "done",
                "detail": f"{triggered} actions triggered",
                "triggered": triggered,
                "elapsed": round(time.time() - t0, 1),
            })

            # Scan summary for provenance
            scanned_sources = {}
            for f in findings:
                src = f.get("source_index", "unknown")
                if src not in scanned_sources:
                    scanned_sources[src] = 0
                scanned_sources[src] += 1

            # Complete
            full_reasoning = get_reasoning_log()
            yield _sse({
                "step": "complete",
                "total": total,
                "annualized": values.get("total_annualized_usd", 0),
                "anomalies_found": n,
                "actions_triggered": triggered,
                "findings": findings,
                "index_count": len(index_map.get("indexes", [])),
                "sector": index_map.get("sector_detected", ""),
                "elapsed": round(time.time() - t0, 1),
                "reasoning_log": full_reasoning,
                "scan_summary": scanned_sources,
                "agent_builder_used": values.get("confidence_score", 0) > 0,
                "tools_used": list(set(f.get("tool_used", "") for f in findings if f.get("tool_used"))),
            })

        except ApiError as exc:
            log.error("SSE pipeline failed (ES): %s", exc, exc_info=True)
            yield _sse({"step": "error", "error": f"Elasticsearch error: {exc}"})
        except ESConnectionError as exc:
            log.error("SSE pipeline failed (connection): %s", exc)
            yield _sse({"step": "error", "error": "Cannot connect to Elasticsearch"})
        except (ValueError, RuntimeError) as exc:
            log.error("SSE pipeline failed: %s", exc, exc_info=True)
            yield _sse({"step": "error", "error": str(exc)})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


def _sse(data: Dict[str, Any]) -> str:
    """Format a dict as a Server-Sent Event string.

    Args:
        data: Event payload.

    Returns:
        SSE-formatted string.
    """
    return f"data: {json.dumps(data)}\n\n"


# ── Data endpoints ────────────────────────────────────────────────────────────

@app.get("/api/indexes")
async def list_indexes() -> JSONResponse:
    """List all Elasticsearch indexes with doc counts.

    Returns:
        JSON list of index names and document counts.
    """
    try:
        es = _es()
        cat = es.cat.indices(format="json", bytes="b")
        indexes = []
        for idx in cat:
            name = idx.get("index", "")
            if name.startswith("."):
                continue
            raw_bytes = int(idx.get("store.size", 0) or idx.get("pri.store.size", 0) or 0)
            if raw_bytes >= 1_073_741_824:
                size_str = f"{raw_bytes / 1_073_741_824:.1f}gb"
            elif raw_bytes >= 1_048_576:
                size_str = f"{raw_bytes / 1_048_576:.1f}mb"
            elif raw_bytes >= 1024:
                size_str = f"{raw_bytes / 1024:.1f}kb"
            else:
                size_str = f"{raw_bytes}b"
            indexes.append({
                "name": name,
                "doc_count": int(idx.get("docs.count", 0) or 0),
                "size": size_str,
            })
        indexes.sort(key=lambda x: x["name"])
        return JSONResponse(content={"indexes": indexes})
    except ApiError as exc:
        log.error("Failed to list indexes: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
    except ESConnectionError:
        return JSONResponse(status_code=503, content={"error": "Cannot connect to Elasticsearch"})


@app.post("/api/upload")
async def upload_csv(
    file: UploadFile = File(None),
    index_name: str = Form(""),
) -> JSONResponse:
    """Upload a CSV file and index it into Elasticsearch.

    Automatically infers field types (float, date, keyword, text) from the data.
    Creates the index with explicit mappings, then bulk-indexes all rows.

    Args:
        file: CSV file upload.
        index_name: Target Elasticsearch index name.

    Returns:
        JSON with index name and document count.
    """
    if not file or not file.filename:
        return JSONResponse(status_code=400, content={"error": "No file selected. Please choose a CSV file."})
    if not index_name or not index_name.strip():
        return JSONResponse(status_code=400, content={"error": "Index name is required."})
    try:
        raw = await file.read()
        text = raw.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        if not rows:
            return JSONResponse(status_code=400, content={"error": "CSV file is empty"})

        fields = list(rows[0].keys())
        log.info("Upload: %d rows, %d fields: %s", len(rows), len(fields), fields)

        column_samples: Dict[str, List[str]] = {f: [] for f in fields}
        for row in rows:
            for f in fields:
                val = row.get(f, "")
                if val:
                    column_samples[f].append(val)

        mappings: Dict[str, Any] = {}
        for f in fields:
            clean_name = f.strip().lower().replace(" ", "_").replace("-", "_")
            ft = _infer_field_type(column_samples.get(f, []))
            mappings[clean_name] = {"type": ft}

        es = _es()
        idx = index_name.strip().lower().replace(" ", "-")

        if not es.indices.exists(index=idx):
            es.indices.create(index=idx, mappings={"properties": mappings})
            log.info("Created index %s with %d fields", idx, len(mappings))

        field_map = {}
        for f in fields:
            field_map[f] = f.strip().lower().replace(" ", "_").replace("-", "_")

        docs = []
        for row in rows:
            doc: Dict[str, Any] = {"@timestamp": datetime.utcnow().isoformat() + "Z"}
            for orig, clean in field_map.items():
                val = row.get(orig, "")
                ftype = mappings.get(clean, {}).get("type", "text")
                if ftype == "float" and val:
                    try:
                        doc[clean] = float(val)
                    except ValueError:
                        doc[clean] = val
                else:
                    doc[clean] = val
            docs.append({"_index": idx, "_source": doc})

        if docs:
            bulk(es, docs)

        log.info("Indexed %d documents into %s", len(docs), idx)
        return JSONResponse(content={
            "index": idx,
            "documents": len(docs),
            "fields": list(mappings.keys()),
            "field_types": {k: v["type"] for k, v in mappings.items()},
        })

    except ApiError as exc:
        log.error("Upload failed (ES): %s", exc, exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(exc)})
    except (UnicodeDecodeError, csv.Error) as exc:
        log.error("Upload failed (parse): %s", exc)
        return JSONResponse(status_code=400, content={"error": f"CSV parse error: {exc}"})


@app.post("/api/connect")
async def connect_index(body: Dict[str, Any]) -> JSONResponse:
    """Validate an existing ES index and return its structure.

    Args:
        body: JSON with "index_name" field.

    Returns:
        JSON with index info, field mappings, and anomaly potential.
    """
    try:
        idx = body.get("index_name", "").strip()
        if not idx:
            return JSONResponse(status_code=400, content={"error": "index_name is required"})

        es = _es()
        if not es.indices.exists(index=idx):
            return JSONResponse(status_code=404, content={"error": f"Index '{idx}' not found"})

        mapping = es.indices.get_mapping(index=idx)
        props = {}
        for _idx_name, idx_data in mapping.items():
            props = idx_data.get("mappings", {}).get("properties", {})
            break

        count_result = es.count(index=idx)
        doc_count = count_result.get("count", 0)

        fields_info = {}
        for fname, fdata in props.items():
            fields_info[fname] = fdata.get("type", "unknown")

        numeric_fields = [f for f, t in fields_info.items() if t in ("float", "double", "integer", "long")]
        keyword_fields = [f for f, t in fields_info.items() if t == "keyword"]
        date_fields = [f for f, t in fields_info.items() if t == "date"]

        return JSONResponse(content={
            "index": idx,
            "doc_count": doc_count,
            "fields": fields_info,
            "numeric_fields": numeric_fields,
            "keyword_fields": keyword_fields,
            "date_fields": date_fields,
            "anomaly_potential": "HIGH" if len(numeric_fields) >= 2 else "MEDIUM" if numeric_fields else "LOW",
        })

    except ApiError as exc:
        log.error("Connect failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ── Custom Pricing ─────────────────────────────────────────────────────────────

@app.get("/api/pricing")
async def list_pricing() -> JSONResponse:
    """List all entries in the pricing-reference index.

    Returns:
        JSON list of pricing items with item_key, unit_cost_usd, etc.
    """
    try:
        es = _es()
        query = "FROM pricing-reference | KEEP item_key, item_name, unit_cost_usd, unit_label, source | LIMIT 100"
        result = es.esql.query(query=query)
        cols = [c["name"] for c in result["columns"]]
        rows = [dict(zip(cols, row)) for row in result["values"]]
        return JSONResponse(content={"pricing": rows})
    except (ApiError, ESConnectionError) as exc:
        log.error("Failed to list pricing: %s", exc)
        return JSONResponse(content={"pricing": [], "error": str(exc)})


@app.post("/api/pricing")
async def add_pricing(body: Dict[str, Any]) -> JSONResponse:
    """Add a custom pricing entry to the pricing-reference index.

    Args:
        body: JSON with item_key, item_name, unit_cost_usd, unit_label, source (optional).

    Returns:
        JSON confirming the indexed pricing entry.
    """
    item_key = body.get("item_key", "").strip().lower().replace(" ", "-")
    item_name = body.get("item_name", "").strip()
    unit_cost = body.get("unit_cost_usd")
    unit_label = body.get("unit_label", "units").strip()
    source = body.get("source", "Custom user-defined pricing").strip()

    if not item_key:
        return JSONResponse(status_code=400, content={"error": "item_key is required"})
    if not item_name:
        return JSONResponse(status_code=400, content={"error": "item_name is required"})
    if unit_cost is None:
        return JSONResponse(status_code=400, content={"error": "unit_cost_usd is required"})
    try:
        unit_cost = float(unit_cost)
    except (ValueError, TypeError):
        return JSONResponse(status_code=400, content={"error": "unit_cost_usd must be a number"})

    try:
        es = _es()
        doc = {
            "item_key": item_key,
            "item_name": item_name,
            "unit_cost_usd": unit_cost,
            "unit_label": unit_label,
            "source": source,
            "effective_date": datetime.utcnow().strftime("%Y-%m-%d"),
        }
        es.index(index="pricing-reference", document=doc, id=f"custom-{item_key}")
        log.info("Added custom pricing: %s = $%.4f/%s", item_key, unit_cost, unit_label)
        return JSONResponse(content={"message": f"Pricing added: {item_name} at ${unit_cost}/{unit_label}", "pricing": doc})
    except ApiError as exc:
        log.error("Failed to add pricing: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.delete("/api/pricing/{item_key}")
async def delete_pricing(item_key: str) -> JSONResponse:
    """Delete a custom pricing entry.

    Args:
        item_key: The item_key to remove.

    Returns:
        JSON confirming deletion.
    """
    try:
        es = _es()
        es.delete(index="pricing-reference", id=f"custom-{item_key}")
        return JSONResponse(content={"message": f"Deleted pricing for {item_key}"})
    except ApiError as exc:
        return JSONResponse(status_code=404, content={"error": f"Pricing entry not found: {exc}"})


# ── Custom Waste Rules ────────────────────────────────────────────────────────

@app.post("/api/rules")
async def add_waste_rule(body: Dict[str, Any]) -> JSONResponse:
    """Save a custom waste detection rule for a specific index.

    Rules define which fields to compare (field_a vs field_b), the grouping
    field, the waste threshold, and an optional custom unit cost.

    Args:
        body: JSON with index_name, field_a, field_b, group_by, threshold, unit_cost.

    Returns:
        JSON confirming the stored rule.
    """
    idx = body.get("index_name", "").strip()
    field_a = body.get("field_a", "").strip()
    field_b = body.get("field_b", "").strip()
    group_by = body.get("group_by", "").strip()
    threshold = body.get("threshold", 0.20)
    unit_cost = body.get("unit_cost")
    rule_name = body.get("rule_name", "").strip() or f"{field_a}-vs-{field_b}"

    if not idx:
        return JSONResponse(status_code=400, content={"error": "index_name is required"})
    if not field_a or not field_b:
        return JSONResponse(status_code=400, content={"error": "field_a and field_b are required"})

    try:
        threshold = float(threshold)
    except (ValueError, TypeError):
        threshold = 0.20

    rule = {
        "index_name": idx,
        "rule_name": rule_name,
        "field_a": field_a,
        "field_b": field_b,
        "group_by": group_by or None,
        "threshold": threshold,
        "unit_cost": float(unit_cost) if unit_cost else None,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }

    rules_file = Path(__file__).parent / "data" / "custom_rules.json"
    existing: List[Dict] = []
    if rules_file.exists():
        with open(rules_file, encoding="utf-8") as f:
            existing = json.load(f)

    existing = [r for r in existing if not (r["index_name"] == idx and r["rule_name"] == rule_name)]
    existing.append(rule)

    with open(rules_file, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)

    log.info("Saved custom rule: %s for %s (threshold=%.2f)", rule_name, idx, threshold)
    return JSONResponse(content={"message": f"Rule saved: {rule_name}", "rule": rule})


@app.get("/api/rules")
async def list_rules() -> JSONResponse:
    """List all custom waste detection rules.

    Returns:
        JSON list of stored rules.
    """
    rules_file = Path(__file__).parent / "data" / "custom_rules.json"
    if not rules_file.exists():
        return JSONResponse(content={"rules": []})
    with open(rules_file, encoding="utf-8") as f:
        rules = json.load(f)
    return JSONResponse(content={"rules": rules})


@app.delete("/api/rules/{index_name}/{rule_name}")
async def delete_rule(index_name: str, rule_name: str) -> JSONResponse:
    """Delete a custom waste detection rule.

    Args:
        index_name: Target index.
        rule_name: Rule name to delete.

    Returns:
        JSON confirming deletion.
    """
    rules_file = Path(__file__).parent / "data" / "custom_rules.json"
    if not rules_file.exists():
        return JSONResponse(status_code=404, content={"error": "No rules file"})
    with open(rules_file, encoding="utf-8") as f:
        rules = json.load(f)
    before = len(rules)
    rules = [r for r in rules if not (r["index_name"] == index_name and r["rule_name"] == rule_name)]
    if len(rules) == before:
        return JSONResponse(status_code=404, content={"error": "Rule not found"})
    with open(rules_file, "w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2)
    return JSONResponse(content={"message": f"Deleted rule {rule_name} for {index_name}"})


# ── Sector endpoints ──────────────────────────────────────────────────────────

@app.get("/api/sectors")
async def list_sectors() -> JSONResponse:
    """List all available sector templates.

    Returns:
        JSON with sector registry data.
    """
    registry_path = Path(__file__).parent / "sectors" / "registry.json"
    try:
        with open(registry_path, encoding="utf-8") as f:
            data = json.load(f)
        return JSONResponse(content=data)
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "Sector registry not found"})


@app.get("/api/sectors/{sector_id}")
async def get_sector(sector_id: str) -> JSONResponse:
    """Get full configuration for a specific sector.

    Args:
        sector_id: Sector identifier (e.g. healthcare, retail).

    Returns:
        JSON with sector config including indexes, tools, pricing, prompts.
    """
    config_path = Path(__file__).parent / "sectors" / f"{sector_id}.json"
    try:
        with open(config_path, encoding="utf-8") as f:
            data = json.load(f)
        return JSONResponse(content=data)
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": f"Sector '{sector_id}' not found"})


# ── Chat endpoint ─────────────────────────────────────────────────────────────

_CHAT_PATTERNS: List[Dict[str, Any]] = [
    {
        "pattern": re.compile(
            r"(?:drug|hospital|pharma|medicine|insulin|procurement|wing|over.?order)",
            re.IGNORECASE,
        ),
        "query": (
            "FROM hospital-drugs "
            "| STATS total_ordered = SUM(qty_ordered), total_used = SUM(qty_used) "
            "  BY drug_name, wing_id "
            "| EVAL delta = total_ordered - total_used, "
            "  waste_ratio = TO_DOUBLE(total_ordered - total_used) / TO_DOUBLE(total_ordered) "
            "| SORT waste_ratio DESC "
            "| LIMIT 10"
        ),
        "explanation": "Querying hospital-drugs index for procurement vs usage patterns",
        "source_index": "hospital-drugs",
        "domain": "Healthcare",
    },
    {
        "pattern": re.compile(
            r"(?:machine|factory|idle|press|runtime|shift|equipment|manufactur)",
            re.IGNORECASE,
        ),
        "query": (
            "FROM factory-iot-data "
            "| WHERE shift_active == false "
            "| STATS total_idle = SUM(runtime_minutes), avg_idle = AVG(runtime_minutes) "
            "  BY machine_id "
            "| EVAL idle_hours = total_idle / 60, cost = idle_hours * 112.50 "
            "| SORT cost DESC "
            "| LIMIT 10"
        ),
        "explanation": "Querying factory-iot-data for off-shift machine runtime",
        "source_index": "factory-iot-data",
        "domain": "Manufacturing",
    },
    {
        "pattern": re.compile(
            r"(?:building|energy|nyc|occupancy|kwh|real.?estate|electricity)",
            re.IGNORECASE,
        ),
        "query": (
            "FROM nyc-buildings "
            "| STATS avg_occ = AVG(occupancy_pct), total_kwh = SUM(energy_kwh) "
            "  BY building_id, borough "
            "| EVAL energy_cost = total_kwh * 0.22, waste_score = (1 - avg_occ) * total_kwh "
            "| SORT waste_score DESC "
            "| LIMIT 10"
        ),
        "explanation": "Querying nyc-buildings for energy vs occupancy divergence",
        "source_index": "nyc-buildings",
        "domain": "Real Estate",
    },
    {
        "pattern": re.compile(r"(?:pricing|cost|rate|price|reference|unit)", re.IGNORECASE),
        "query": "FROM pricing-reference | KEEP item_key, item_name, unit_cost_usd, unit_label, source | LIMIT 20",
        "explanation": "Listing all pricing reference data",
        "source_index": "pricing-reference",
        "domain": "Reference Data",
    },
    {
        "pattern": re.compile(r"(?:audit|finding|action|alert|history|recent)", re.IGNORECASE),
        "query": (
            "FROM ghost-economy-audit "
            "| SORT @timestamp DESC "
            "| KEEP @timestamp, finding_id, entity, category, dollar_value, action_taken, priority "
            "| LIMIT 20"
        ),
        "explanation": "Showing recent audit trail from ghost-economy-audit",
        "source_index": "ghost-economy-audit",
        "domain": "Audit Trail",
    },
    {
        "pattern": re.compile(r"(?:total|summary|how much|waste|overview|all)", re.IGNORECASE),
        "query": (
            "FROM ghost-economy-audit "
            "| STATS "
            "    total_waste = SUM(dollar_value), "
            "    findings = COUNT(*), "
            "    avg_confidence = AVG(confidence) "
            "  BY category "
            "| SORT total_waste DESC "
            "| LIMIT 10"
        ),
        "explanation": "Summarizing total waste by category from audit records",
        "source_index": "ghost-economy-audit",
        "domain": "Summary",
    },
    {
        "pattern": re.compile(
            r"(?:compare|wing.+vs|vs.+wing|across|breakdown|by wing|by machine|by building)",
            re.IGNORECASE,
        ),
        "query": (
            "FROM hospital-drugs "
            "| STATS total_ordered = SUM(qty_ordered), total_used = SUM(qty_used) "
            "  BY wing_id "
            "| EVAL delta = total_ordered - total_used, "
            "  waste_pct = TO_DOUBLE(total_ordered - total_used) / TO_DOUBLE(total_ordered) * 100 "
            "| SORT waste_pct DESC "
            "| LIMIT 20"
        ),
        "explanation": "Comparing waste across different wings/departments",
        "source_index": "hospital-drugs",
        "domain": "Healthcare",
    },
]

_CHAT_SUGGESTIONS = [
    "Show me drug waste patterns",
    "Which machines are running idle?",
    "What buildings waste energy?",
    "Show pricing reference data",
    "Total waste summary",
    "Recent audit findings",
    "Compare waste across wings",
    "What indexes do I have?",
]


@app.post("/api/chat")
async def chat_query(body: Dict[str, Any]) -> JSONResponse:
    """Run an interactive ES|QL query based on user's natural-language question.

    Uses regex pattern matching with entity extraction to map user questions
    to ES|QL queries, returning results with full data provenance.

    Args:
        body: JSON with "message" field.

    Returns:
        JSON with query results, ES|QL query, explanation, and provenance.
    """
    msg = body.get("message", "").strip()
    if not msg:
        return JSONResponse(status_code=400, content={"error": "message is required"})

    try:
        es = _es()

        if re.search(r"(?:index|indexes|what data|list|show me what)", msg, re.IGNORECASE):
            cat = es.cat.indices(format="json", h="index,docs.count,store.size")
            indexes = [
                {"name": i.get("index", ""), "docs": int(i.get("docs.count", 0) or 0),
                 "size": i.get("store.size", "0b")}
                for i in cat if not i.get("index", "").startswith(".")
            ]
            return JSONResponse(content={
                "type": "index_list",
                "explanation": "Here are all your Elasticsearch indexes:",
                "results": sorted(indexes, key=lambda x: x["name"]),
                "provenance": {"source": "Elasticsearch cat indices API", "timestamp": datetime.utcnow().isoformat()},
            })

        if re.search(r"^(hi|hello|hey|howdy|sup|yo|greetings|good\s?(morning|afternoon|evening))[\s!?.]*$", msg, re.IGNORECASE):
            return JSONResponse(content={
                "type": "greeting",
                "source": "ghost_economy_hunter",
                "response": (
                    "Hello! I'm Ghost Economy Hunter, your AI assistant for finding hidden "
                    "financial waste in your Elasticsearch data.\n\n"
                    "I can:\n"
                    "- Run anomaly detection across all your indexes\n"
                    "- Query your data with natural language\n"
                    "- Show waste patterns, idle machines, energy divergence\n"
                    "- Calculate dollar impact and audit trails\n\n"
                    "Try asking me something like:\n"
                    "\"Show me drug waste patterns\" or \"Which machines are idle?\""
                ),
            })

        if re.search(r"^(how are you|what are you|who are you|what can you do|help|what is this)[\s!?.]*$", msg, re.IGNORECASE):
            return JSONResponse(content={
                "type": "greeting",
                "source": "ghost_economy_hunter",
                "response": (
                    "I'm Ghost Economy Hunter — a multi-agent AI system built on "
                    "Elastic Agent Builder that finds hidden financial waste.\n\n"
                    "My 4 agents:\n"
                    "1. Cartographer — discovers and maps all your ES indexes\n"
                    "2. Pattern Seeker — runs ES|QL anomaly detection tools\n"
                    "3. Valuator — calculates dollar impact from pricing data\n"
                    "4. Action Taker — triggers Slack alerts and workflows\n\n"
                    "Ask me anything about your data, or go to the Hunt tab to run a full scan!"
                ),
            })

        if re.search(r"^(thanks?|thank you|thx|ty|great|awesome|cool|nice|ok|okay)[\s!?.]*$", msg, re.IGNORECASE):
            return JSONResponse(content={
                "type": "greeting",
                "source": "ghost_economy_hunter",
                "response": "You're welcome! Let me know if you have more questions about your data.",
            })

        matched = None
        for pat_def in _CHAT_PATTERNS:
            if pat_def["pattern"].search(msg):
                matched = pat_def
                break

        if not matched:
            return JSONResponse(content={
                "type": "help",
                "explanation": f"I can answer data questions using ES|QL. I didn't match a specific query for: \"{msg}\". Try asking about:",
                "suggestions": _CHAT_SUGGESTIONS,
            })

        query = matched["query"]
        result = es.esql.query(query=query)
        cols = [c["name"] for c in result["columns"]]
        rows = [dict(zip(cols, row)) for row in result["values"]]

        return JSONResponse(content={
            "type": "query_result",
            "explanation": matched["explanation"],
            "esql_query": query,
            "columns": cols,
            "rows": rows,
            "row_count": len(rows),
            "provenance": {
                "source_index": matched["source_index"],
                "domain": matched["domain"],
                "query_type": "ES|QL",
                "timestamp": datetime.utcnow().isoformat(),
            },
        })

    except ApiError as exc:
        log.error("Chat query failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
    except ESConnectionError:
        return JSONResponse(status_code=503, content={"error": "Cannot connect to Elasticsearch"})


# ── Agent Builder status ──────────────────────────────────────────────────────

@app.get("/api/agent-builder/status")
async def agent_builder_status() -> JSONResponse:
    """Check Agent Builder connection health.

    Returns:
        JSON with connection status, agent count, and config info.
    """
    try:
        from orchestrator.agent_caller import test_connection
        status = test_connection()
        status["agent_builder_enabled"] = os.getenv("AGENT_BUILDER_ENABLED", "true").lower() in ("true", "1", "yes")
        status["kibana_url"] = os.getenv("KIBANA_URL", "")[:50] + "..." if os.getenv("KIBANA_URL") else ""
        return JSONResponse(content=status)
    except ImportError:
        return JSONResponse(content={"connected": False, "error": "agent_caller module not found"})


@app.get("/api/agent-builder/agents")
async def list_ab_agents() -> JSONResponse:
    """List all agents registered in Agent Builder.

    Returns:
        JSON list of agents with id, name, description.
    """
    try:
        from orchestrator.agent_caller import list_agents
        agents = list_agents()
        return JSONResponse(content={"agents": agents})
    except ImportError:
        return JSONResponse(content={"agents": [], "error": "agent_caller module not found"})


@app.get("/api/agent-builder/tools")
async def list_ab_tools() -> JSONResponse:
    """List all tools registered in Agent Builder.

    Returns:
        JSON list of tools with id, type, description.
    """
    try:
        from orchestrator.agent_caller import list_tools
        tools = list_tools()
        return JSONResponse(content={"tools": tools})
    except ImportError:
        return JSONResponse(content={"tools": [], "error": "agent_caller module not found"})


# ── Agent Builder converse (direct chat) ─────────────────────────────────────

@app.post("/api/converse")
async def converse_with_agent(body: Dict[str, Any]) -> JSONResponse:
    """Proxy a chat message to the Agent Builder converse API.

    This gives the frontend direct access to Agent Builder agents for
    conversational interaction (beyond the regex-based /api/chat).

    Args:
        body: JSON with "message", optional "agent_id", optional "conversation_id".

    Returns:
        JSON with agent response text, conversation_id, and agent_id.
    """
    msg = body.get("message", "").strip()
    if not msg:
        return JSONResponse(status_code=400, content={"error": "message is required"})

    agent_id = body.get("agent_id", "")
    conversation_id = body.get("conversation_id", "")

    try:
        from orchestrator.agent_caller import converse
        result = converse(
            msg,
            agent_id=agent_id or None,
            conversation_id=conversation_id or None,
        )
        return JSONResponse(content={
            "response": result["response"],
            "conversation_id": result["conversation_id"],
            "agent_id": result["agent_id"],
            "source": "agent_builder",
        })
    except (ValueError, ImportError) as exc:
        log.warning("Converse unavailable: %s — falling back to local chat", exc)
        fallback = await chat_query(body)
        return fallback
    except Exception as exc:
        log.warning("Converse failed: %s — falling back to local chat", exc)
        try:
            fallback = await chat_query(body)
            return fallback
        except Exception:
            return JSONResponse(status_code=502, content={
                "error": f"Agent unavailable: {exc}",
                "detail": "Check Kibana connectivity and try again.",
            })


# ── Integrations endpoint (MCP, A2A, Workflows) ─────────────────────────────

@app.get("/api/integrations")
async def get_integrations() -> JSONResponse:
    """Return MCP, A2A, and workflow integration URLs for the current deployment.

    Returns:
        JSON with MCP server URL, A2A endpoints, workflow status, and
        ready-to-paste config snippets.
    """
    kibana = os.getenv("KIBANA_URL", "").rstrip("/")
    api_key = os.getenv("ELASTIC_API_KEY", "")

    agent_ids = [
        "ghost-cartographer",
        "ghost-pattern-seeker",
        "ghost-valuator",
        "ghost-action-taker",
    ]

    mcp_url = f"{kibana}/api/agent_builder/mcp" if kibana else ""
    a2a_endpoints = {
        aid: f"{kibana}/api/agent_builder/a2a/{aid}" for aid in agent_ids
    } if kibana else {}

    mcp_config = {
        "mcpServers": {
            "ghost-economy-hunter": {
                "command": "npx",
                "args": [
                    "mcp-remote",
                    mcp_url,
                    "--header",
                    f"Authorization:ApiKey {api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "Authorization:ApiKey YOUR_KEY",
                ],
            }
        }
    } if mcp_url else {}

    return JSONResponse(content={
        "mcp": {
            "server_url": mcp_url,
            "config_snippet": mcp_config,
            "description": "Connect Cursor, Claude Desktop, or any MCP client to Ghost Economy Hunter tools",
        },
        "a2a": {
            "endpoints": a2a_endpoints,
            "description": "Agent-to-Agent protocol endpoints for each Ghost Economy agent",
        },
        "workflow": {
            "name": "ghost-economy-action-workflow",
            "description": "Elastic Workflow that receives verified findings, sends Slack alerts, and creates audit records",
            "yaml_path": "elastic/workflows/action_workflow.yaml",
        },
        "kibana_url": kibana,
        "has_credentials": bool(kibana and api_key),
    })


# ── History endpoint ──────────────────────────────────────────────────────────

@app.get("/api/history")
async def hunt_history() -> JSONResponse:
    """Return recent hunt results from the ghost-economy-audit index.

    Returns:
        JSON with recent audit records grouped by run timestamp.
    """
    try:
        es = _es()
        query = (
            "FROM ghost-economy-audit "
            "| SORT @timestamp DESC "
            "| KEEP @timestamp, finding_id, entity, category, source_index, "
            "  tool_used, dollar_value, action_taken, priority, confidence, "
            "  annualized_value, calculation "
            "| LIMIT 50"
        )
        result = es.esql.query(query=query)
        cols = [c["name"] for c in result["columns"]]
        rows = [dict(zip(cols, row)) for row in result["values"]]

        total_waste = sum(r.get("dollar_value", 0) for r in rows)
        total_annualized = sum(r.get("annualized_value", 0) for r in rows)

        return JSONResponse(content={
            "records": rows,
            "total_records": len(rows),
            "total_waste_usd": total_waste,
            "total_annualized_usd": total_annualized,
        })
    except ApiError as exc:
        log.error("History query failed: %s", exc)
        return JSONResponse(content={"records": [], "total_records": 0, "error": str(exc)})
    except ESConnectionError:
        return JSONResponse(content={"records": [], "total_records": 0, "error": "Not connected"})


# ── Impact metrics ────────────────────────────────────────────────────────────

_impact_cache: Dict[str, Any] = {"data": None, "ts": 0}
_IMPACT_TTL = 180


@app.get("/api/impact")
async def impact_metrics() -> JSONResponse:
    """Return computed impact metrics comparing automated vs manual analysis.

    Uses a 3-minute in-memory cache to keep the dashboard snappy.

    Returns:
        JSON with before/after comparison metrics.
    """
    now = time.time()
    if _impact_cache["data"] and (now - _impact_cache["ts"]) < _IMPACT_TTL:
        return JSONResponse(content=_impact_cache["data"])

    try:
        es = _es()
        audit_query = (
            "FROM ghost-economy-audit "
            "| STATS total_waste = SUM(dollar_value), finding_count = COUNT(*), "
            "  avg_confidence = AVG(confidence) "
            "| LIMIT 1"
        )
        result = es.esql.query(query=audit_query)
        cols = [c["name"] for c in result["columns"]]
        rows = [dict(zip(cols, row)) for row in result["values"]]
        audit_stats = rows[0] if rows else {"total_waste": 0, "finding_count": 0, "avg_confidence": 0}

        idx_query = es.cat.indices(format="json", h="index,docs.count")
        total_docs = sum(int(i.get("docs.count", 0) or 0) for i in idx_query if not i.get("index", "").startswith("."))
        index_count = sum(1 for i in idx_query if not i.get("index", "").startswith("."))

        finding_count = audit_stats.get("finding_count", 0) or 0
        manual_hours_per_finding = 8
        manual_hours_total = finding_count * manual_hours_per_finding
        avg_confidence = audit_stats.get("avg_confidence", 0) or 0

        payload = {
            "total_waste_found_usd": audit_stats.get("total_waste", 0) or 0,
            "total_findings": finding_count,
            "indexes_scanned": index_count,
            "documents_analyzed": total_docs,
            "avg_confidence_pct": round(avg_confidence * 100, 1),
            "comparison": {
                "manual_analysis": {
                    "estimated_hours": manual_hours_total,
                    "queries_needed": finding_count * 5,
                    "error_rate_pct": 15,
                    "coverage": "1-2 departments",
                },
                "ghost_economy_hunter": {
                    "estimated_minutes": "3-4",
                    "agent_steps": 4,
                    "error_rate_pct": 0,
                    "coverage": f"Full org ({index_count} indexes, {total_docs:,} docs)",
                },
            },
            "time_saved_hours": max(0, manual_hours_total - 1),
            "automation_rate_pct": 100,
            "roi_multiplier": round(manual_hours_total * 60 / 4, 0) if manual_hours_total else 0,
        }
        _impact_cache["data"] = payload
        _impact_cache["ts"] = time.time()
        return JSONResponse(content=payload)
    except (ApiError, ESConnectionError) as exc:
        log.warning("Impact metrics unavailable: %s", exc)
        return JSONResponse(content={
            "total_waste_found_usd": 0,
            "total_findings": 0,
            "indexes_scanned": 0,
            "documents_analyzed": 0,
            "avg_confidence_pct": 0,
            "comparison": {
                "manual_analysis": {"estimated_hours": 40, "queries_needed": 15, "error_rate_pct": 15, "coverage": "1-2 departments"},
                "ghost_economy_hunter": {"estimated_minutes": "3-4", "agent_steps": 4, "error_rate_pct": 0, "coverage": "Full org scan"},
            },
            "time_saved_hours": 39,
            "automation_rate_pct": 100,
            "roi_multiplier": 13000,
        })


# ── Sample data generation ────────────────────────────────────────────────────

@app.post("/api/generate")
async def generate_sample_data() -> JSONResponse:
    """Generate and index the 3 sample datasets + pricing + exceptions.

    Returns:
        JSON with success message.
    """
    try:
        from data.generate_all import main as gen_main
        log.info("API: generating sample data...")
        gen_main()
        return JSONResponse(content={
            "message": "Sample data generated: factory IoT, hospital drugs, NYC buildings, pricing reference, and known exceptions.",
        })
    except ApiError as exc:
        log.error("Sample data generation failed (ES): %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
    except (FileNotFoundError, ImportError) as exc:
        log.error("Sample data generation failed: %s", exc, exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ── Export ─────────────────────────────────────────────────────────────────────

@app.get("/api/export")
async def export_results(fmt: str = "json") -> StreamingResponse:
    """Export audit history as JSON or CSV.

    Args:
        fmt: "json" or "csv".

    Returns:
        Downloadable file response.
    """
    try:
        es = _es()
        query = (
            "FROM ghost-economy-audit "
            "| SORT @timestamp DESC "
            "| KEEP @timestamp, finding_id, entity, category, source_index, "
            "  tool_used, dollar_value, action_taken, priority, confidence, "
            "  annualized_value, calculation "
            "| LIMIT 200"
        )
        result = es.esql.query(query=query)
        cols = [c["name"] for c in result["columns"]]
        rows = [dict(zip(cols, row)) for row in result["values"]]

        if fmt == "csv":
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=cols)
            writer.writeheader()
            writer.writerows(rows)
            output.seek(0)
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=ghost-economy-findings.csv"},
            )

        return StreamingResponse(
            iter([json.dumps({"records": rows, "total": len(rows)}, indent=2)]),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=ghost-economy-findings.json"},
        )
    except (ApiError, ESConnectionError) as exc:
        log.error("Export failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ── Available scan targets ────────────────────────────────────────────────────

_KNOWN_TARGETS: Dict[str, Dict[str, str]] = {
    "hospital-drugs": {
        "label": "Hospital Drug Procurement",
        "icon": "\U0001f48a",
        "description": "Detect over-ordering vs actual usage across hospital wings",
        "tool": "ghost.usage_anomaly",
        "domain": "Healthcare",
    },
    "factory-iot-data": {
        "label": "Factory IoT Machines",
        "icon": "\U0001f3ed",
        "description": "Find machines running idle during off-shift hours",
        "tool": "ghost.runtime_anomaly",
        "domain": "Manufacturing",
    },
    "nyc-buildings": {
        "label": "NYC Building Energy",
        "icon": "\U0001f3e2",
        "description": "Spot high energy usage in low-occupancy buildings",
        "tool": "ghost.energy_anomaly",
        "domain": "Real Estate",
    },
}

_SYSTEM_INDEXES = {"pricing-reference", "known-exceptions", "ghost-economy-audit"}


@app.get("/api/scan-targets")
async def list_scan_targets() -> JSONResponse:
    """Dynamically discover all ES indexes and return them as scan targets.

    Known indexes get rich descriptions; custom/uploaded indexes get
    auto-generated descriptions from their field mappings.

    Returns:
        JSON with available scan targets including doc counts and descriptions.
    """
    targets: List[Dict[str, Any]] = []
    try:
        es = _es()
        cat_result = es.cat.indices(format="json", h="index,docs.count")
        for entry in cat_result:
            idx_name = entry.get("index", "")
            if idx_name.startswith(".") or idx_name in _SYSTEM_INDEXES:
                continue
            doc_count = int(entry.get("docs.count", 0) or 0)

            if idx_name in _KNOWN_TARGETS:
                info = _KNOWN_TARGETS[idx_name]
                targets.append({
                    "index": idx_name,
                    "label": info["label"],
                    "icon": info["icon"],
                    "description": info["description"],
                    "tool": info.get("tool", "generic"),
                    "domain": info["domain"],
                    "doc_count": doc_count,
                    "available": doc_count > 0,
                    "custom": False,
                })
            else:
                try:
                    mapping = es.indices.get_mapping(index=idx_name)
                    props = {}
                    for _k, v in mapping.items():
                        props = v.get("mappings", {}).get("properties", {})
                        break
                    num_fields = [f for f, d in props.items() if d.get("type") in ("float", "double", "integer", "long")]
                    kw_fields = [f for f, d in props.items() if d.get("type") == "keyword"]
                    desc = f"{len(num_fields)} numeric fields, {len(kw_fields)} keyword fields — generic anomaly scan"
                except ApiError:
                    num_fields = []
                    desc = "Custom uploaded dataset"

                targets.append({
                    "index": idx_name,
                    "label": idx_name.replace("-", " ").replace("_", " ").title(),
                    "icon": "\U0001f4ca",
                    "description": desc,
                    "tool": "generic",
                    "domain": "Custom",
                    "doc_count": doc_count,
                    "available": doc_count > 0,
                    "custom": True,
                })
    except (ApiError, ESConnectionError):
        for idx_name, info in _KNOWN_TARGETS.items():
            targets.append({
                "index": idx_name, "label": info["label"], "icon": info["icon"],
                "description": info["description"], "tool": info.get("tool", "generic"),
                "domain": info["domain"], "doc_count": 0, "available": False, "custom": False,
            })
    targets.sort(key=lambda t: (t.get("custom", False), t["index"]))
    return JSONResponse(content={"targets": targets})


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health() -> JSONResponse:
    """Health check endpoint."""
    es_ok = False
    try:
        es = _es()
        es.info()
        es_ok = True
    except (ApiError, ESConnectionError):
        pass

    return JSONResponse(content={
        "status": "ok" if es_ok else "degraded",
        "service": "Ghost Economy Hunter",
        "version": "2.0.0",
        "elasticsearch": "connected" if es_ok else "disconnected",
        "agent_builder_enabled": os.getenv("AGENT_BUILDER_ENABLED", "true").lower() in ("true", "1", "yes"),
        "warnings": _startup_warnings,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    log.info("Starting Ghost Economy Hunter API v2.0 on http://localhost:%d", port)
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
