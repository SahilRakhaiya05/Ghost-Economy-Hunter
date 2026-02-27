# Ghost Economy Hunter

**A 4-agent AI system built on Elastic Agent Builder that autonomously finds hidden financial waste inside any organization's Elasticsearch data.**

> Built for the [Elastic Agent Builder Hackathon 2026](https://elasticsearch.devpost.com)

---

## What It Does

Every organization has money hiding in its own data — drugs ordered but never used, machines running during off-shifts, empty buildings consuming full power. Ghost Economy Hunter finds it automatically.

**The pipeline:**

| Agent | Role | Tool |
|---|---|---|
| Cartographer | Maps all Elasticsearch indexes | Built-in index introspection |
| Pattern Seeker | Finds waste anomalies via ES\|QL | `usage_anomaly`, `runtime_anomaly`, `energy_anomaly` |
| Valuator | Assigns dollar values to every finding | `value_calculator` |
| Action Taker | Verifies findings, fires Elastic Workflow | `trigger_action_workflow` |

**In our demo across 3 industries, it found $2.2M+ in hidden waste in under 90 seconds.**

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/SahilRakhaiya05/Ghost-Economy-Hunter.git
cd Ghost-Economy-Hunter
python -m venv .venv
.venv\Scripts\activate      # Windows
# source .venv/bin/activate  # Mac/Linux
pip install -r requirements.txt
```

### 2. Configure credentials

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```
ELASTIC_URL=https://your-project.es.us-east-1.aws.elastic.cloud
ELASTIC_API_KEY=your_api_key_here
KIBANA_URL=https://your-project.kb.us-east-1.aws.elastic.cloud
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### 3. Create indexes

```bash
python elastic/setup/create_indexes.py
```

### 4. Generate data

```bash
python data/generate_all.py
```

### 5. Run the live demo

```bash
python api.py
```

Open [http://localhost:8000](http://localhost:8000) — click **Start Hunt** to run the real pipeline against your Elasticsearch data.

### 6. Run the pipeline directly (CLI)

```bash
python -m orchestrator.main
```

---

## Project Structure

```
ghost-economy-hunter/
├── api.py                      # FastAPI server — serves frontend + /api/run endpoint
├── constants.py                # Index names, agent IDs
├── requirements.txt
├── .env.example
│
├── data/
│   ├── generate_all.py         # Convenience: generates all 3 domains at once
│   ├── generate_factory.py     # Factory IoT (90 days, 3 machines, idle anomaly)
│   ├── generate_hospital.py    # Hospital drugs (180 days, Wing-C over-ordering)
│   ├── fetch_nyc_buildings.py  # NYC buildings (365 days, BLDG-047 energy waste)
│   └── pricing_reference.json  # Unit cost reference data
│
├── elastic/
│   ├── setup/
│   │   ├── create_indexes.py   # Creates 6 indexes with explicit mappings
│   │   └── index_data.py       # Bulk indexing pipeline
│   ├── tools/
│   │   ├── usage-anomaly.json       # ES|QL: drug over-procurement
│   │   ├── runtime-anomaly.json     # ES|QL: idle machine detection
│   │   ├── energy-anomaly.json      # ES|QL: building energy waste
│   │   └── value-calculator.json    # ES|QL: pricing lookup
│   ├── agents/
│   │   ├── cartographer.json
│   │   ├── pattern-seeker.json
│   │   ├── valuator.json
│   │   └── action-taker.json
│   └── workflows/
│       └── action_workflow.yaml     # Elastic Workflow: Slack + audit record
│
├── orchestrator/
│   ├── main.py                 # 4-agent pipeline (direct ES|QL execution)
│   ├── elastic_client.py       # Elasticsearch client factory
│   ├── agent_caller.py         # Agent Builder API wrapper
│   └── value_formatter.py      # Currency formatting helpers
│
├── frontend/
│   └── index.html              # Animated demo UI (calls /api/run for live data)
│
└── dashboard/
    └── kibana_dashboard.json   # Kibana dashboard export
```

---

## Indexes

| Index | Domain | Records |
|---|---|---|
| `factory-iot-data` | Manufacturing IoT sensor readings | ~8,100 |
| `hospital-drugs` | Drug procurement vs consumption | ~8,640 |
| `nyc-buildings` | Building occupancy + energy usage | ~1,095 |
| `pricing-reference` | Unit cost lookup table | 5 |
| `known-exceptions` | Approved exception registry | 0 |
| `ghost-economy-audit` | Pipeline output / audit trail | grows per run |

---

## ES|QL Queries

All anomaly detection uses pure ES|QL. Example:

```sql
FROM hospital-drugs
| STATS
    total_ordered = SUM(qty_ordered),
    total_used    = SUM(qty_used)
  BY drug_name, wing_id
| EVAL
    delta       = total_ordered - total_used,
    waste_ratio = TO_DOUBLE(total_ordered - total_used) / TO_DOUBLE(total_ordered)
| WHERE waste_ratio > 0.25
| SORT waste_ratio DESC
| LIMIT 20
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Serves the frontend HTML |
| `POST` | `/api/run` | Runs the full 4-agent pipeline, returns JSON results |
| `GET` | `/api/health` | Health check |

---

## Tech Stack

- **Python 3.11+** — orchestration, data generation
- **elasticsearch-py 8.x** — Elasticsearch client
- **ES|QL** — all anomaly detection queries
- **Elastic Agent Builder** — 4 agents + 4 tools in Kibana
- **Elastic Workflows** — Slack alert + audit record automation
- **FastAPI + uvicorn** — lightweight API server
- **requests, numpy, faker** — HTTP, data generation

---

## License

MIT — see [LICENSE](LICENSE)
