"""Ghost Economy Hunter — FastAPI server.

Serves the frontend and exposes endpoints for:
  - /api/run       — execute the 4-agent pipeline
  - /api/upload    — upload a CSV file to create a new ES index
  - /api/connect   — connect an existing ES index for scanning
  - /api/indexes   — list all available indexes
  - /api/health    — health check

Run with:
    python api.py
Then open: http://localhost:8000
"""
from __future__ import annotations

import csv
import io
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent))

import json as _json

import uvicorn
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError
from elasticsearch.helpers import bulk
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ghost-economy-api")

_ES_URL = os.getenv("ELASTIC_URL", "")
_ES_KEY = os.getenv("ELASTIC_API_KEY", "")

app = FastAPI(
    title="Ghost Economy Hunter API",
    description="Runs the 4-agent pipeline against live Elasticsearch data.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

FRONTEND_DIR = Path(__file__).parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


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


@app.get("/", include_in_schema=False)
async def serve_frontend() -> FileResponse:
    """Serve the main frontend HTML."""
    return FileResponse(str(FRONTEND_DIR / "index.html"))


@app.post("/api/run")
async def run_pipeline() -> JSONResponse:
    """Execute the full Ghost Economy Hunt pipeline.

    Returns:
        JSON with real pipeline results from live Elasticsearch data.
    """
    try:
        from orchestrator.main import run_hunt
        log.info("API: starting pipeline run...")
        results = run_hunt()

        findings = []
        for f in results.get("values", {}).get("valued_findings", []):
            findings.append({
                "id":       f.get("anomaly_id", ""),
                "entity":   f.get("entity", ""),
                "category": f.get("category", ""),
                "dollar":   f.get("dollar_value", 0),
                "calc":     f.get("calculation", ""),
                "priority": f.get("priority", "MEDIUM"),
            })

        payload = {
            "total":             results.get("total_ghost_economy_usd", 0),
            "anomalies_found":   results.get("anomalies_found", 0),
            "actions_triggered": results.get("actions_triggered", 0),
            "findings":          findings,
            "summary":           results.get("values", {}).get("summary", ""),
            "index_count":       len(results.get("index_map", {}).get("indexes", [])),
        }
        log.info("API: pipeline complete — total=%s", payload["total"])
        return JSONResponse(content=payload)

    except Exception as exc:  # noqa: BLE001
        log.error("Pipeline failed: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(exc), "detail": "Pipeline execution failed"},
        )


@app.get("/api/run/stream")
async def run_pipeline_stream():
    """Execute the pipeline with Server-Sent Events for real-time progress.

    Streams events as each agent completes so the frontend can update live.

    Returns:
        SSE stream with agent progress events.
    """
    import time

    def event_stream():
        """Generator yielding SSE events as agents execute."""
        try:
            from orchestrator.main import (
                run_cartographer, run_pattern_seeker,
                run_valuator, run_action_taker,
            )
            from orchestrator.value_formatter import format_dollar

            t0 = time.time()

            yield _sse({"step": 1, "agent": "Cartographer", "status": "running"})
            index_map = run_cartographer()
            yield _sse({
                "step": 1, "agent": "Cartographer", "status": "done",
                "detail": f"{len(index_map.get('indexes', []))} indexes mapped",
                "sector": index_map.get("sector_detected", ""),
                "elapsed": round(time.time() - t0, 1),
            })

            yield _sse({"step": 2, "agent": "Pattern Seeker", "status": "running"})
            patterns = run_pattern_seeker(index_map)
            n = patterns.get("total_anomalies_found", 0)
            yield _sse({
                "step": 2, "agent": "Pattern Seeker", "status": "done",
                "detail": f"{n} anomalies found",
                "anomalies": n,
                "elapsed": round(time.time() - t0, 1),
            })

            yield _sse({"step": 3, "agent": "Valuator", "status": "running"})
            values = run_valuator(patterns)
            total = values.get("total_ghost_economy_usd", 0)
            findings = []
            for f in values.get("valued_findings", []):
                findings.append({
                    "id": f.get("anomaly_id", ""),
                    "entity": f.get("entity", ""),
                    "category": f.get("category", ""),
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

            yield _sse({"step": 4, "agent": "Action Taker", "status": "running"})
            actions = run_action_taker(values)
            triggered = actions.get("actions_triggered", 0)
            yield _sse({
                "step": 4, "agent": "Action Taker", "status": "done",
                "detail": f"{triggered} actions triggered",
                "triggered": triggered,
                "elapsed": round(time.time() - t0, 1),
            })

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
            })

        except Exception as exc:
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
    return f"data: {_json.dumps(data)}\n\n"


@app.get("/api/indexes")
async def list_indexes() -> JSONResponse:
    """List all Elasticsearch indexes with doc counts.

    Returns:
        JSON list of index names and document counts.
    """
    try:
        es = _es()
        cat = es.cat.indices(format="json", h="index,docs.count,store.size")
        indexes = []
        for idx in cat:
            name = idx.get("index", "")
            if name.startswith("."):
                continue
            indexes.append({
                "name": name,
                "doc_count": int(idx.get("docs.count", 0) or 0),
                "size": idx.get("store.size", "0b"),
            })
        indexes.sort(key=lambda x: x["name"])
        return JSONResponse(content={"indexes": indexes})
    except ApiError as exc:
        log.error("Failed to list indexes: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.post("/api/upload")
async def upload_csv(
    file: UploadFile = File(...),
    index_name: str = Form(...),
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
            es.indices.create(index=idx, body={"mappings": {"properties": mappings}})
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

    except Exception as exc:  # noqa: BLE001
        log.error("Upload failed: %s", exc, exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.post("/api/connect")
async def connect_index(body: Dict[str, Any]) -> JSONResponse:
    """Validate an existing ES index and return its structure.

    Args:
        body: JSON with "index_name" field.

    Returns:
        JSON with index info, field mappings, and sample data.
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


@app.get("/api/sectors")
async def list_sectors() -> JSONResponse:
    """List all available sector templates.

    Returns:
        JSON with sector registry data.
    """
    registry_path = Path(__file__).parent / "sectors" / "registry.json"
    try:
        with open(registry_path, encoding="utf-8") as f:
            import json as _json
            data = _json.load(f)
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
            import json as _json
            data = _json.load(f)
        return JSONResponse(content=data)
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": f"Sector '{sector_id}' not found"})


@app.post("/api/chat")
async def chat_query(body: Dict[str, Any]) -> JSONResponse:
    """Run an interactive ES|QL query based on user's natural-language question.

    Matches user questions to pre-built ES|QL patterns and returns live results.

    Args:
        body: JSON with "message" field.

    Returns:
        JSON with query results and explanation.
    """
    msg = body.get("message", "").strip().lower()
    if not msg:
        return JSONResponse(status_code=400, content={"error": "message is required"})

    try:
        es = _es()
        query = None
        explanation = ""

        if any(w in msg for w in ["drug", "hospital", "pharma", "medicine", "insulin", "procurement"]):
            query = (
                "FROM hospital-drugs "
                "| STATS total_ordered = SUM(qty_ordered), total_used = SUM(qty_used) "
                "  BY drug_name, wing_id "
                "| EVAL delta = total_ordered - total_used, "
                "  waste_ratio = TO_DOUBLE(total_ordered - total_used) / TO_DOUBLE(total_ordered) "
                "| SORT waste_ratio DESC "
                "| LIMIT 10"
            )
            explanation = "Querying hospital-drugs index for procurement vs usage patterns"

        elif any(w in msg for w in ["machine", "factory", "idle", "press", "runtime", "shift"]):
            query = (
                "FROM factory-iot-data "
                "| WHERE shift_active == false "
                "| STATS total_idle = SUM(runtime_minutes), avg_idle = AVG(runtime_minutes) "
                "  BY machine_id "
                "| EVAL idle_hours = total_idle / 60, cost = idle_hours * 112.50 "
                "| SORT cost DESC "
                "| LIMIT 10"
            )
            explanation = "Querying factory-iot-data for off-shift machine runtime"

        elif any(w in msg for w in ["building", "energy", "nyc", "occupancy", "kwh", "real estate"]):
            query = (
                "FROM nyc-buildings "
                "| STATS avg_occ = AVG(occupancy_pct), total_kwh = SUM(energy_kwh) "
                "  BY building_id, borough "
                "| EVAL energy_cost = total_kwh * 0.22, waste_score = (1 - avg_occ) * total_kwh "
                "| SORT waste_score DESC "
                "| LIMIT 10"
            )
            explanation = "Querying nyc-buildings for energy vs occupancy divergence"

        elif any(w in msg for w in ["pricing", "cost", "rate", "price", "reference"]):
            query = "FROM pricing-reference | KEEP item_key, item_name, unit_cost_usd, unit_label, source | LIMIT 20"
            explanation = "Listing all pricing reference data"

        elif any(w in msg for w in ["audit", "finding", "action", "alert", "history"]):
            query = (
                "FROM ghost-economy-audit "
                "| SORT @timestamp DESC "
                "| KEEP @timestamp, finding_id, entity, category, dollar_value, action_taken, priority "
                "| LIMIT 20"
            )
            explanation = "Showing recent audit trail from ghost-economy-audit"

        elif any(w in msg for w in ["total", "summary", "how much", "waste", "overview"]):
            query = (
                "FROM ghost-economy-audit "
                "| STATS "
                "    total_waste = SUM(dollar_value), "
                "    findings = COUNT(*), "
                "    avg_confidence = AVG(confidence) "
                "  BY category "
                "| SORT total_waste DESC "
                "| LIMIT 10"
            )
            explanation = "Summarizing total waste by category from audit records"

        elif any(w in msg for w in ["index", "indexes", "what data", "list", "show me"]):
            cat = es.cat.indices(format="json", h="index,docs.count,store.size")
            indexes = [
                {"name": i.get("index", ""), "docs": int(i.get("docs.count", 0) or 0)}
                for i in cat if not i.get("index", "").startswith(".")
            ]
            return JSONResponse(content={
                "type": "index_list",
                "explanation": "Here are all your Elasticsearch indexes:",
                "results": sorted(indexes, key=lambda x: x["name"]),
            })

        else:
            return JSONResponse(content={
                "type": "help",
                "explanation": "I can answer questions about your data. Try asking about:",
                "suggestions": [
                    "Show me drug waste patterns",
                    "Which machines are running idle?",
                    "What buildings waste energy?",
                    "Show pricing reference data",
                    "Total waste summary",
                    "Recent audit findings",
                    "What indexes do I have?",
                ],
            })

        result = es.esql.query(query=query)
        cols = [c["name"] for c in result["columns"]]
        rows = [dict(zip(cols, row)) for row in result["values"]]

        return JSONResponse(content={
            "type": "query_result",
            "explanation": explanation,
            "esql_query": query,
            "columns": cols,
            "rows": rows,
            "row_count": len(rows),
        })

    except ApiError as exc:
        log.error("Chat query failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


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
        return JSONResponse(content={"message": "Sample data generated: factory IoT, hospital drugs, NYC buildings, pricing reference, and known exceptions."})
    except Exception as exc:  # noqa: BLE001
        log.error("Sample data generation failed: %s", exc, exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.get("/api/health")
async def health() -> JSONResponse:
    """Health check endpoint."""
    return JSONResponse(content={"status": "ok", "service": "Ghost Economy Hunter"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    log.info("Starting Ghost Economy Hunter API on http://localhost:%d", port)
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
