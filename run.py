"""Ghost Economy Hunter -- Single Command Launcher.

One command to rule them all:
    python run.py

This script:
  1. Validates .env configuration
  2. Checks Elasticsearch connectivity
  3. Creates indexes with mappings (if missing)
  4. Generates sample data (if indexes are empty)
  5. Provisions Agent Builder tools + agents (if Kibana is reachable)
  6. Starts the FastAPI server on port 8000
  7. Opens the browser automatically
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time
import webbrowser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ghost-launcher")

REQUIRED_ENV = ["ELASTIC_URL", "ELASTIC_API_KEY"]
OPTIONAL_ENV = ["KIBANA_URL", "SLACK_WEBHOOK_URL"]


def _banner() -> None:
    """Print the startup banner."""
    print("\n" + "=" * 60)
    print("   GHOST ECONOMY HUNTER")
    print("   Multi-Agent AI System for Finding Hidden Financial Waste")
    print("=" * 60)


def _check_env() -> bool:
    """Validate required environment variables.

    Returns:
        True if all required vars are set.
    """
    ok = True
    for var in REQUIRED_ENV:
        val = os.getenv(var, "")
        if not val:
            log.error("MISSING: %s — set it in .env", var)
            ok = False
        else:
            log.info("  %s = %s...%s", var, val[:12], val[-4:] if len(val) > 16 else "")
    for var in OPTIONAL_ENV:
        val = os.getenv(var, "")
        if val:
            log.info("  %s = configured", var)
        else:
            log.warning("  %s = not set (optional)", var)
    return ok


def _check_elasticsearch() -> bool:
    """Test Elasticsearch connectivity.

    Returns:
        True if connected.
    """
    try:
        from elasticsearch import Elasticsearch
        from elasticsearch.exceptions import ConnectionError as ESConnErr
        es = Elasticsearch(
            os.getenv("ELASTIC_URL", ""),
            api_key=os.getenv("ELASTIC_API_KEY", ""),
            request_timeout=10,
        )
        info = es.info()
        log.info("  Elasticsearch connected: %s (v%s)",
                 info.get("cluster_name", "unknown"),
                 info.get("version", {}).get("number", "?"))
        return True
    except ESConnErr:
        log.error("  Cannot connect to Elasticsearch — check ELASTIC_URL and API key")
        return False
    except Exception as exc:
        log.error("  Elasticsearch error: %s", exc)
        return False


def _ensure_indexes() -> None:
    """Create indexes if they don't exist."""
    log.info("Checking indexes...")
    try:
        from elastic.setup.create_indexes import main as create_main
        create_main()
    except Exception as exc:
        log.warning("Index creation: %s (may already exist)", exc)


def _ensure_data() -> None:
    """Generate sample data if indexes are empty."""
    try:
        from elasticsearch import Elasticsearch
        es = Elasticsearch(
            os.getenv("ELASTIC_URL", ""),
            api_key=os.getenv("ELASTIC_API_KEY", ""),
        )
        resp = es.cat.indices(format="json", h="index,docs.count")
        target_indexes = {"factory-iot-data", "hospital-drugs", "nyc-buildings"}
        existing = {
            r["index"]: int(r.get("docs.count", 0) or 0)
            for r in resp
            if r.get("index") in target_indexes
        }
        empty = [idx for idx in target_indexes if existing.get(idx, 0) == 0]
        if empty:
            log.info("Empty indexes found: %s — generating sample data...", ", ".join(empty))
            from data.generate_all import main as gen_main
            gen_main()
        else:
            log.info("  All data indexes have documents — skipping generation")
    except Exception as exc:
        log.warning("Data check/generation: %s", exc)


def _provision_agents() -> None:
    """Provision tools and agents into Agent Builder (if Kibana is available)."""
    kibana = os.getenv("KIBANA_URL", "")
    if not kibana:
        log.info("  KIBANA_URL not set — skipping Agent Builder provisioning")
        return
    log.info("Provisioning Agent Builder tools and agents...")
    try:
        from elastic.setup.provision_agents import provision_all
        provision_all(do_verify=False)
    except SystemExit:
        log.warning("  Provisioning exited (check KIBANA_URL and API key)")
    except Exception as exc:
        log.warning("  Provisioning: %s (Agent Builder may not be available)", exc)


def _kill_existing_server(port: int) -> None:
    """Try to kill any existing process on the given port (Windows).

    Args:
        port: The port to free up.
    """
    import subprocess
    try:
        result = subprocess.run(
            ["netstat", "-ano"], capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.splitlines():
            if f":{port}" in line and "LISTENING" in line:
                parts = line.strip().split()
                pid = parts[-1]
                if pid.isdigit() and int(pid) != os.getpid():
                    subprocess.run(["taskkill", "/PID", pid, "/F"],
                                   capture_output=True, timeout=5)
                    log.info("Killed existing server on port %d (PID %s)", port, pid)
                    time.sleep(1)
                    return
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass


def _find_free_port(preferred: int) -> int:
    """Find a free port, starting from the preferred one.

    Args:
        preferred: The preferred port number to try first.

    Returns:
        A free port number.
    """
    import socket
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("0.0.0.0", preferred))
            return preferred
    except OSError:
        _kill_existing_server(preferred)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("0.0.0.0", preferred))
                return preferred
        except OSError:
            pass

    for port in range(preferred + 1, preferred + 20):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("0.0.0.0", port))
                log.warning("Port %d in use, using %d instead", preferred, port)
                return port
        except OSError:
            continue
    log.error("No free port found in range %d-%d", preferred, preferred + 19)
    return preferred


def _open_browser(port: int) -> None:
    """Open the browser after a short delay.

    Args:
        port: The server port.
    """
    time.sleep(2)
    url = f"http://localhost:{port}"
    log.info("Opening browser: %s", url)
    webbrowser.open(url)


def main() -> None:
    """Run the full startup sequence."""
    _banner()

    log.info("\n[1/5] Checking environment...")
    if not _check_env():
        log.error("\nFix .env and try again. See .env.example for reference.")
        sys.exit(1)

    log.info("\n[2/5] Testing Elasticsearch connection...")
    if not _check_elasticsearch():
        log.error("\nCannot proceed without Elasticsearch. Check ELASTIC_URL and ELASTIC_API_KEY.")
        sys.exit(1)

    log.info("\n[3/5] Ensuring indexes exist...")
    _ensure_indexes()

    log.info("\n[4/5] Checking data and provisioning agents...")
    _ensure_data()
    _provision_agents()

    log.info("\n[5/5] Starting server...")
    port = int(os.getenv("PORT", "8000"))
    port = _find_free_port(port)

    browser_thread = threading.Thread(target=_open_browser, args=(port,), daemon=True)
    browser_thread.start()

    print("\n" + "=" * 60)
    print(f"   Ghost Economy Hunter is running at http://localhost:{port}")
    print("   Press Ctrl+C to stop")
    print("=" * 60 + "\n")

    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)


if __name__ == "__main__":
    main()
