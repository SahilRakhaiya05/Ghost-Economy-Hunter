# Ghost Economy Hunter — Demo Script

> **Total time: 5 minutes**
> This script is designed so you (or an AI) can deliver a compelling, story-driven demo.
> Every section has: what to show on screen, what to say, and exact timing.

---

## Before You Start

```bash
pip install -r requirements.txt
# Set up .env with your Elastic Cloud credentials
python run.py
# Browser opens automatically to http://localhost:8000
```

Make sure all 4 status dots on the Dashboard are green before starting.

---

## ACT 1: The Story (45 seconds)

> **Screen:** Dashboard tab is open. Don't click anything yet.

### What to Say

> *"Imagine you're the CFO of a hospital chain. Your team just submitted the annual budget — $48 million in drug procurement alone. But here's what nobody tells you: between 15% and 25% of that is waste. Drugs ordered that were never used. Insulin that expired on the shelf. Amoxicillin sitting in a wing that doesn't need it."*
>
> *"Now multiply that across every department. Your factories are running machines at 3 AM with nobody watching. Your buildings are blasting AC into empty floors. This hidden waste has a name — we call it the Ghost Economy."*
>
> *"The problem? Finding it takes an army of analysts, 40+ hours of manual SQL queries, and they still miss 85% of it."*

**Pause. Point at the comparison table on screen.**

> *"What if you could find all of it — in under 4 minutes — with zero manual queries?"*

---

## ACT 2: One Command (15 seconds)

> **Screen:** Still on Dashboard. Point at the green status dots.

### What to Say

> *"Everything you see here started with one command: `python run.py`. That single command connected to Elasticsearch, created 6 indexes, loaded real data — including actual NYC building data from the city government's Open Data API — registered 4 AI agents and 4 ES|QL tools in Agent Builder, and launched this dashboard. All automatic."*

---

## ACT 3: The Hunt (90 seconds)

> **Screen:** Click the **Hunt** tab now.

### Step 3a — Select Data (10 sec)

> Click **"Select All"** to select all data sources.

> *"We have hospital drug records, factory IoT sensor data, NYC building energy data, and a retail inventory dataset. Let's scan everything."*

### Step 3b — Run (5 sec)

> Click **"Run Hunt"**.

> *"One click. Four agents are about to work."*

### Step 3c — Watch Agents Work (60 sec)

> **Screen:** Watch the 4 pipeline steps light up one by one. Click "Show reasoning" on each agent as it runs.

**Agent 1 lights up (blue):**
> *"Agent 1 — the Cartographer — just discovered 7 indexes in our cluster. It mapped every field, detected that we have healthcare, manufacturing, and real estate domains."*

**Agent 2 lights up (purple):**
> *"Agent 2 — the Pattern Seeker — is now running ES|QL anomaly queries. Watch the reasoning panel — you can see the actual queries being executed. It's using specialized tools: one for drug over-procurement, one for idle machines, one for energy waste. For the retail data we uploaded, it auto-built a generic scanner."*

**Agent 3 lights up (green):**
> *"Agent 3 — the Valuator — is looking up real prices. That insulin number? It's $212.50 per unit — the actual CMS Medicare price, not a guess. Every dollar value you'll see comes from official government pricing data."*

**Agent 4 lights up (amber):**
> *"Agent 4 — the Action Taker — just verified the findings, sent Slack alerts for the critical ones, and created audit records in Elasticsearch. End-to-end."*

### Step 3d — Results (15 sec)

> **Screen:** Scroll down to the findings table.

> *"Look at the results. Every row has an entity, the source index it came from, the category of waste, the exact dollar value, an annualized projection, the actual calculation, and a priority level. This isn't a summary — it's an itemized audit backed by real ES|QL queries."*

> Point at the total at the bottom right.

> *"Total hidden waste found: [read the number]. In under 4 minutes."*

---

## ACT 4: Real Data Proof (30 seconds)

> **Screen:** Click the **Chat** tab.

### Step 4a — Ask a Question

> Type: **"Show me drug waste patterns"** and press Enter.

> *"I can also talk to the system in natural language. Watch — it converts my question into an ES|QL query and runs it live against Elasticsearch."*

> **Screen:** Results appear with a table and the actual ES|QL query shown.

> *"See the query it wrote? `FROM hospital-drugs | STATS SUM ordered, SUM used BY drug_name, wing_id | WHERE waste_ratio > 0.25`. That's a real ES|QL query running on real data right now. No LLM hallucination — deterministic, auditable results."*

---

## ACT 5: Any Data Source (45 seconds)

> **Screen:** Click the **Connect Data** tab.

### Step 5a — Show Upload

> *"Here's what makes this different from a one-trick demo. Ghost Economy Hunter works with any data."*

> Point at the Upload CSV section.

> *"See this? I can take a CSV export from any system — an ERP, an IoT platform, an HR system, a financial report — upload it, and the agents automatically scan it for waste patterns."*

### Step 5b — Show Existing Indexes

> Scroll down to "Current Indexes" section.

> *"We already have a retail inventory dataset loaded — 2,000 records from 10 stores. The generic scanner auto-detected the numeric fields, found mismatch patterns between `units_received` and `units_sold`, and calculated spoilage waste. No configuration. No schema mapping. Just upload and scan."*

### Step 5c — The Power Statement

> *"Hospital data. Factory sensors. NYC government buildings. Retail inventory. Four completely different domains. Same 4 agents found waste in all of them."*

---

## ACT 6: Under the Hood (30 seconds)

> **Screen:** Click the **Architecture** tab.

> *"Everything runs on Elastic Agent Builder. Four agents and four ES|QL tools — all registered via the Kibana API."*

> Point at the live agent cards and tool cards.

> *"These aren't mock-ups. These are live from your Elastic cluster right now. The agents support MCP — so you can connect Cursor or Claude Desktop directly — and A2A protocol for agent-to-agent communication."*

> Click the **ES|QL Console** tab.

> *"And here's every ES|QL query that was executed during the hunt. Fully transparent. Every result is traceable back to the exact query that produced it."*

---

## ACT 7: The Close (30 seconds)

> **Screen:** Go back to the **Dashboard** tab.

### What to Say

> *"Let me recap what just happened."*
>
> *"One command started the entire system. Four AI agents — built on Elastic Agent Builder — scanned data from 4 different industries. They used parameterized ES|QL tools to find anomalies, calculated dollar impact using real government pricing, sent Slack alerts, and created a full audit trail."*
>
> *"A manual audit of this same data would take 40 hours. We did it in [read elapsed time]. That's not a demo — that's the future of operational intelligence."*
>
> *"This is Ghost Economy Hunter."*

---

## If Judges Ask Questions

### "Is this real data?"

> *"Yes. The NYC building data comes from the city's official Open Data API — Local Law 84 energy disclosures. The retail dataset is a real inventory export from 10 stores. Pricing comes from CMS Medicare, NADAC, BLS Producer Price Index, and EIA electricity rates — all official government sources. The hospital and factory data are synthetic but modeled on realistic distributions from those same government datasets."*

### "Can it work with our data?"

> *"Absolutely. Upload any CSV in the Connect Data tab. The system auto-detects field types, creates an Elasticsearch index with proper mappings, and the generic scanner finds anomaly patterns automatically. No configuration needed."*

### "How does Agent Builder fit in?"

> *"All 4 agents and all 4 ES|QL tools are registered in Elastic Agent Builder via the Kibana REST API. The agents use the `/api/agent_builder/converse` endpoint. Each agent has a system prompt with ROLE, MISSION, TOOLS, OUTPUT FORMAT, and CONSTRAINTS sections. They also expose MCP and A2A endpoints for external integration."*

### "What about false positives?"

> *"We have a `known-exceptions` index. The Action Taker agent checks every finding against it before triggering alerts. We also score every finding with a confidence score between 0 and 1 — only high-confidence findings trigger Slack notifications. Everything else still gets logged to the audit trail for manual review."*

### "What ES|QL features do you use?"

> *"Parameterized queries with `?threshold` parameters, `STATS ... BY` aggregations, `EVAL` for computed fields, `WHERE` for filtering, `SORT` for ranking, and `LIMIT` for result sets. All queries follow the pattern: `FROM index | WHERE ... | STATS ... BY ... | EVAL ... | SORT ... | LIMIT N`."*

### "How are agents orchestrated?"

> *"The orchestrator runs a sequential pipeline: Cartographer outputs feed into Pattern Seeker, which feeds into Valuator, which feeds into Action Taker. Each agent call goes through `orchestrator/agent_caller.py` using the Agent Builder converse API. Results are streamed to the frontend via Server-Sent Events so the user sees progress in real-time."*

### "What would you build next?"

> *"Three things: (1) Scheduled hunts — run every hour automatically and only alert on new findings. (2) Cross-index correlation — the Cartographer already identifies correlation pairs, we'd run joint anomaly queries across paired indexes. (3) A Kibana plugin that embeds Ghost Economy Hunter directly into the Elastic dashboard."*

---

## Quick Reference Card

| What | Value |
|------|-------|
| Startup command | `python run.py` |
| URL | `http://localhost:8000` |
| Agents | 4 (Cartographer, Pattern Seeker, Valuator, Action Taker) |
| ES\|QL Tools | 4 (usage-anomaly, runtime-anomaly, energy-anomaly, value-calculator) |
| Data Sources | NYC Open Data (real), Retail CSV (real), Hospital (synthetic), Factory (synthetic) |
| Pricing Sources | CMS Medicare, CMS NADAC, BLS PPI, EIA |
| API Endpoints | 16 |
| Pipeline Time | ~3-4 minutes |
| Frontend | Single HTML file, 7 tabs, dark/light mode |
