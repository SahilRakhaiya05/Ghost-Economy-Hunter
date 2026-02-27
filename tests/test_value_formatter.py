"""Tests for orchestrator.value_formatter module."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.value_formatter import format_dollar, format_dollar_range


class TestFormatDollar:
    def test_basic_formatting(self) -> None:
        assert format_dollar(1234.56) == "$1,234.56"

    def test_zero(self) -> None:
        assert format_dollar(0) == "$0.00"

    def test_large_value(self) -> None:
        assert format_dollar(2_231_416.13) == "$2,231,416.13"

    def test_none_returns_zero(self) -> None:
        assert format_dollar(None) == "$0.00"

    def test_negative(self) -> None:
        assert format_dollar(-500) == "$-500.00"

    def test_integer_input(self) -> None:
        assert format_dollar(100) == "$100.00"

    def test_string_non_numeric(self) -> None:
        assert format_dollar("abc") == "$0.00"  # type: ignore[arg-type]


class TestFormatDollarRange:
    def test_basic_range(self) -> None:
        result = format_dollar_range(1000, 2000)
        assert result == "$1,000.00 \u2013 $2,000.00"

    def test_same_values(self) -> None:
        result = format_dollar_range(500, 500)
        assert result == "$500.00 \u2013 $500.00"
