"""Ghost Economy Hunter — index and agent ID constants. Never hardcode these elsewhere."""

# Index names (kebab-case)
INDEX_FACTORY_IOT = "factory-iot-data"
INDEX_HOSPITAL_DRUGS = "hospital-drugs"
INDEX_NYC_BUILDINGS = "nyc-buildings"
INDEX_PRICING_REFERENCE = "pricing-reference"
INDEX_KNOWN_EXCEPTIONS = "known-exceptions"
INDEX_GHOST_ECONOMY_AUDIT = "ghost-economy-audit"

# Agent Builder agent IDs — provisioned via _provision_kibana.py
AGENT_CARTOGRAPHER   = "ghost.cartographer"
AGENT_PATTERN_SEEKER = "ghost.pattern_seeker"
AGENT_VALUATOR       = "ghost.valuator"
AGENT_ACTION_TAKER   = "ghost.action_taker"

MAX_ANOMALIES = 1000
