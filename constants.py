"""Ghost Economy Hunter — index, tool, and agent ID constants. Never hardcode these elsewhere."""

# Index names (kebab-case)
INDEX_FACTORY_IOT = "factory-iot-data"
INDEX_HOSPITAL_DRUGS = "hospital-drugs"
INDEX_NYC_BUILDINGS = "nyc-buildings"
INDEX_PRICING_REFERENCE = "pricing-reference"
INDEX_KNOWN_EXCEPTIONS = "known-exceptions"
INDEX_GHOST_ECONOMY_AUDIT = "ghost-economy-audit"

# Agent Builder agent IDs — match the "id" field in elastic/agents/*.json
AGENT_CARTOGRAPHER = "ghost-cartographer"
AGENT_PATTERN_SEEKER = "ghost-pattern-seeker"
AGENT_VALUATOR = "ghost-valuator"
AGENT_ACTION_TAKER = "ghost-action-taker"

# Agent Builder tool IDs — match the "id" field in elastic/tools/*.json
TOOL_USAGE_ANOMALY = "ghost-usage-anomaly"
TOOL_RUNTIME_ANOMALY = "ghost-runtime-anomaly"
TOOL_ENERGY_ANOMALY = "ghost-energy-anomaly"
TOOL_VALUE_CALCULATOR = "ghost-value-calculator"

MAX_ANOMALIES = 1000
