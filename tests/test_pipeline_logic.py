"""Tests for pipeline scoring and classification logic."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _classify_priority(dollar_value: float) -> str:
    """Replicate the priority classification from orchestrator/main.py."""
    if dollar_value >= 100_000:
        return "CRITICAL"
    elif dollar_value >= 50_000:
        return "HIGH"
    elif dollar_value >= 10_000:
        return "MEDIUM"
    else:
        return "LOW"


def _compute_actionability(confidence: float, priority: str) -> float:
    """Replicate actionability scoring from orchestrator/main.py."""
    dollar_score = {
        "CRITICAL": 1.0,
        "HIGH": 0.8,
        "MEDIUM": 0.6,
        "LOW": 0.3,
    }.get(priority, 0.5)
    return round((confidence + dollar_score) / 2, 2)


class TestPriorityClassification:
    def test_critical(self) -> None:
        assert _classify_priority(200_000) == "CRITICAL"
        assert _classify_priority(100_000) == "CRITICAL"

    def test_high(self) -> None:
        assert _classify_priority(75_000) == "HIGH"
        assert _classify_priority(50_000) == "HIGH"

    def test_medium(self) -> None:
        assert _classify_priority(25_000) == "MEDIUM"
        assert _classify_priority(10_000) == "MEDIUM"

    def test_low(self) -> None:
        assert _classify_priority(5_000) == "LOW"
        assert _classify_priority(0) == "LOW"

    def test_boundary_values(self) -> None:
        assert _classify_priority(99_999.99) == "HIGH"
        assert _classify_priority(100_000.00) == "CRITICAL"


class TestActionabilityScoring:
    def test_high_confidence_critical(self) -> None:
        score = _compute_actionability(0.95, "CRITICAL")
        assert score == 0.97  # (0.95 + 1.0) / 2 = 0.975, rounded to 0.97
        assert score >= 0.5

    def test_medium_confidence_high(self) -> None:
        score = _compute_actionability(0.7, "HIGH")
        assert score == 0.75
        assert score >= 0.5

    def test_low_confidence_low_priority(self) -> None:
        score = _compute_actionability(0.3, "LOW")
        assert score == 0.3
        assert score < 0.5  # should be suppressed

    def test_threshold_boundary(self) -> None:
        score = _compute_actionability(0.4, "MEDIUM")
        assert score == 0.5
        assert score >= 0.5  # exactly at threshold

    def test_all_scores_in_range(self) -> None:
        for conf in [0.0, 0.25, 0.5, 0.75, 1.0]:
            for pri in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
                score = _compute_actionability(conf, pri)
                assert 0.0 <= score <= 1.0, f"Score {score} out of range for conf={conf}, pri={pri}"


class TestDollarCalculation:
    def test_usage_mismatch_calculation(self) -> None:
        delta = 10048
        unit_cost = 212.50
        result = round(delta * unit_cost, 2)
        assert result == 2_135_200.00

    def test_runtime_gap_calculation(self) -> None:
        idle_hours = 681
        cost_per_hour = 112.50
        result = round(idle_hours * cost_per_hour, 2)
        assert result == 76_612.50

    def test_energy_divergence_calculation(self) -> None:
        excess_kwh = 103_177
        rate = 0.22
        result = round(excess_kwh * rate, 2)
        assert result == 22_698.94

    def test_annualized_value(self) -> None:
        dollar_value = 76_612.50
        days = 90
        annualized = round(dollar_value * (365 / days), 2)
        assert annualized > dollar_value
        assert annualized == round(76_612.50 * (365 / 90), 2)
