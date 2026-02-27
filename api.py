"""Ghost Economy Hunter — FastAPI server.

Serves the frontend and exposes a single /api/run endpoint
that executes the real 4-agent ES|QL pipeline and streams results.

Run with:
    python api.py
Then open: http://localhost:8000
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ghost-economy-api")

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

# Serve the frontend static files
FRONTEND_DIR = Path(__file__).parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.get("/", include_in_schema=False)
async def serve_frontend() -> FileResponse:
    """Serve the main frontend HTML."""
    index_path = FRONTEND_DIR / "index.html"
    return FileResponse(str(index_path))


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

        # Build a clean response shaped for the frontend
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
            "total":           results.get("total_ghost_economy_usd", 0),
            "anomalies_found": results.get("anomalies_found", 0),
            "actions_triggered": results.get("actions_triggered", 0),
            "findings":        findings,
            "summary":         results.get("values", {}).get("summary", ""),
            "index_count":     len(results.get("index_map", {}).get("indexes", [])),
        }
        log.info("API: pipeline complete — total=%s", payload["total"])
        return JSONResponse(content=payload)

    except Exception as exc:  # noqa: BLE001
        log.error("Pipeline failed: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(exc), "detail": "Pipeline execution failed"},
        )


@app.get("/api/health")
async def health() -> JSONResponse:
    """Health check endpoint."""
    return JSONResponse(content={"status": "ok", "service": "Ghost Economy Hunter"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    log.info("Starting Ghost Economy Hunter API on http://localhost:%d", port)
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
