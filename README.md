# Ghost Economy Hunter

Multi-agent AI system on **Elastic Agent Builder** that finds hidden financial waste inside any organization's Elasticsearch data.

## Tech stack

- **Backend:** Python 3.11+
- **Elastic:** elasticsearch-py 8.x
- **Agent Builder:** Kibana REST API
- **Queries:** ES|QL only
- **Workflows:** Elastic Workflows YAML
- **Config:** python-dotenv (`.env`)

## Setup

1. **Elastic Cloud**
   - Create a free trial at [cloud.elastic.co](https://cloud.elastic.co/registration?cta=agentbuilderhackathon).
   - Create an **Elasticsearch Serverless** project named `ghost-economy-hunter`.
   - Copy the Elasticsearch endpoint and create an API key. Save both in `.env`.

2. **Environment**
   ```bash
   cp .env.example .env
   # Edit .env: ELASTIC_URL, ELASTIC_API_KEY, KIBANA_URL, SLACK_WEBHOOK_URL
   ```

3. **Install**
   ```bash
   pip install -r requirements.txt
   ```

4. **Create indexes**
   ```bash
   python elastic/setup/create_indexes.py
   ```

5. **Index data**
   ```bash
   python elastic/setup/index_data.py
   ```
   Or run generators individually:
   ```bash
   python data/generate_factory.py
   python data/generate_hospital.py
   python data/fetch_nyc_buildings.py
   ```
   Then index pricing reference (via script or Kibana Dev Tools bulk).

6. **Kibana Agent Builder**
   - Create 4 agents (Cartographer, Pattern Seeker, Valuator, Action Taker). Paste instructions from `elastic/agents/*.json` into each agent’s Instructions.
   - Create 4 ES|QL tools from `elastic/tools/*.json` and attach to the correct agents.
   - Create the workflow from `elastic/workflows/action_workflow.yaml`.
   - In Kibana Dev Tools run: `GET kbn:/api/agent_builder/agents` and copy each agent `id` into `constants.py` (AGENT_CARTOGRAPHER, etc.).

7. **Run the hunt**
   ```bash
   python -m orchestrator.main
   ```

## Project layout

- **`.cursorrules`** — Cursor AI rules (tech stack, file structure, coding rules).
- **`constants.py`** — Index names and agent IDs (do not hardcode elsewhere).
- **`data/`** — Data generators and `pricing_reference.json`.
- **`elastic/setup/`** — `create_indexes.py`, `index_data.py`.
- **`elastic/tools/`** — ES|QL tool definitions (usage_anomaly, runtime_anomaly, energy_anomaly, value_calculator).
- **`elastic/agents/`** — Agent instructions for Kibana.
- **`elastic/workflows/`** — `action_workflow.yaml`.
- **`orchestrator/`** — `main.py`, `elastic_client.py`, `agent_caller.py`, `value_formatter.py`.
- **`dashboard/`** — Kibana dashboard panel definitions (import and build in Kibana).

## Kibana dashboard

After runs, build in Kibana:

1. **Total Ghost Economy:** Metric panel, index `ghost-economy-audit`, metric `SUM(dollar_value)`, format currency.
2. **Findings by Category:** Lens pie, slice by `category`, size by `SUM(dollar_value)`.
3. **Detection Timeline:** Lens bar, X `@timestamp`, Y `SUM(dollar_value)`, color `category`.

## Rules

- All secrets in `.env`; never commit `.env`.
- All ES queries in ES|QL; all agent calls via `agent_caller.py`; all dollar formatting via `value_formatter.py`.
- Index names and agent IDs live in `constants.py` only.
