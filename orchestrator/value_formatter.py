"""Format dollar values for user-facing output. Never format currency inline elsewhere."""
from __future__ import annotations

from typing import Optional, Union


def format_dollar(value: Optional[Union[int, float]]) -> str:
    """Format a numeric value as USD with two decimal places.

    Args:
        value: Dollar amount; None or non-numeric becomes '$0.00'.

    Returns:
        Formatted string, e.g. '$1,234.56'.
    """
    if value is None:
        return "$0.00"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "$0.00"
    return f"${num:,.2f}"


def format_dollar_range(low: Union[int, float], high: Union[int, float]) -> str:
    """Format a range of dollar values.

    Args:
        low: Lower bound.
        high: Upper bound.

    Returns:
        String like '$1,000.00 – $2,000.00'.
    """
    return f"{format_dollar(low)} – {format_dollar(high)}"
