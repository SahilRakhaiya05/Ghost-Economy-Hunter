"""Generate a 2000-row retail inventory CSV with embedded anomalies.

Columns: date, store_id, product_sku, category, units_received, units_sold,
         units_wasted, unit_cost

10 stores, 50 products, ~90 days of data.
Store S-07 wastes 40%+ of fresh produce (detectable by generic scanner).
"""
from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

_OUT = Path(__file__).resolve().parent / "sample_retail_inventory.csv"

_STORES = [f"S-{i:02d}" for i in range(1, 11)]

_PRODUCTS = {
    "Fresh Produce": [
        ("FP-001", "Organic Bananas", 2.49),
        ("FP-002", "Roma Tomatoes", 3.99),
        ("FP-003", "Baby Spinach", 4.29),
        ("FP-004", "Avocados", 5.99),
        ("FP-005", "Strawberries", 6.49),
    ],
    "Dairy": [
        ("DA-001", "Whole Milk 1gal", 4.79),
        ("DA-002", "Greek Yogurt", 5.49),
        ("DA-003", "Cheddar Block", 6.99),
        ("DA-004", "Heavy Cream", 3.29),
        ("DA-005", "Butter Unsalted", 4.99),
    ],
    "Bakery": [
        ("BK-001", "Sourdough Loaf", 5.99),
        ("BK-002", "Croissants 4pk", 7.49),
        ("BK-003", "Bagels 6pk", 4.99),
        ("BK-004", "Baguette", 3.49),
        ("BK-005", "Muffins 4pk", 6.29),
    ],
    "Frozen": [
        ("FZ-001", "Frozen Pizza", 8.99),
        ("FZ-002", "Ice Cream Pint", 5.49),
        ("FZ-003", "Frozen Veggies", 2.99),
        ("FZ-004", "Fish Sticks", 7.49),
        ("FZ-005", "Frozen Waffles", 3.99),
    ],
    "Beverages": [
        ("BV-001", "Orange Juice 64oz", 6.49),
        ("BV-002", "Sparkling Water 12pk", 5.99),
        ("BV-003", "Coffee Beans 1lb", 12.99),
        ("BV-004", "Green Tea 20ct", 4.49),
        ("BV-005", "Almond Milk", 3.99),
    ],
    "Snacks": [
        ("SN-001", "Tortilla Chips", 4.29),
        ("SN-002", "Trail Mix 1lb", 8.99),
        ("SN-003", "Granola Bars 8ct", 5.49),
        ("SN-004", "Dark Chocolate Bar", 3.99),
        ("SN-005", "Popcorn 3pk", 4.49),
    ],
    "Meat": [
        ("MT-001", "Chicken Breast 2lb", 9.99),
        ("MT-002", "Ground Beef 1lb", 7.49),
        ("MT-003", "Pork Chops", 8.99),
        ("MT-004", "Turkey Deli Sliced", 6.49),
        ("MT-005", "Bacon 12oz", 7.99),
    ],
    "Pantry": [
        ("PT-001", "Pasta 1lb", 1.99),
        ("PT-002", "Canned Tomatoes", 2.49),
        ("PT-003", "Olive Oil 500ml", 8.99),
        ("PT-004", "Rice 2lb", 3.49),
        ("PT-005", "Peanut Butter", 4.99),
    ],
    "Household": [
        ("HH-001", "Paper Towels 6pk", 11.99),
        ("HH-002", "Dish Soap", 3.49),
        ("HH-003", "Trash Bags 30ct", 8.99),
        ("HH-004", "Sponges 3pk", 2.99),
        ("HH-005", "Laundry Det 64oz", 12.49),
    ],
    "Health": [
        ("HL-001", "Multivitamins 90ct", 14.99),
        ("HL-002", "Hand Sanitizer", 3.99),
        ("HL-003", "Band-Aids 30ct", 5.49),
        ("HL-004", "Pain Reliever 100ct", 9.99),
        ("HL-005", "Toothpaste", 4.49),
    ],
}

_PERISHABLE = {"Fresh Produce", "Dairy", "Bakery", "Meat"}


def generate() -> None:
    """Write sample_retail_inventory.csv with 2000 rows and anomalies.

    Returns:
        None
    """
    rng = random.Random(42)
    start = date(2025, 10, 1)
    rows: list[dict[str, str]] = []

    all_products = [(cat, sku, name, cost) for cat, items in _PRODUCTS.items() for sku, name, cost in items]

    while len(rows) < 2000:
        day = start + timedelta(days=rng.randint(0, 89))
        store = rng.choice(_STORES)
        cat, sku, _name, cost = rng.choice(all_products)

        received = rng.randint(20, 120)
        is_perishable = cat in _PERISHABLE

        if store == "S-07" and cat == "Fresh Produce":
            waste_pct = rng.uniform(0.40, 0.65)
            wasted = max(1, int(received * waste_pct))
            sold = max(0, received - wasted - rng.randint(0, 3))
        elif is_perishable:
            waste_pct = rng.uniform(0.02, 0.10)
            wasted = max(0, int(received * waste_pct))
            sold = max(0, received - wasted - rng.randint(0, 5))
        else:
            wasted = rng.randint(0, max(1, int(received * 0.02)))
            sold = max(0, received - wasted - rng.randint(0, 3))

        rows.append({
            "date": day.isoformat(),
            "store_id": store,
            "product_sku": sku,
            "category": cat,
            "units_received": str(received),
            "units_sold": str(sold),
            "units_wasted": str(wasted),
            "unit_cost": f"{cost:.2f}",
        })

    with open(_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "date", "store_id", "product_sku", "category",
            "units_received", "units_sold", "units_wasted", "unit_cost",
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {_OUT}")

    s07_produce = [r for r in rows if r["store_id"] == "S-07" and r["category"] == "Fresh Produce"]
    if s07_produce:
        total_recv = sum(int(r["units_received"]) for r in s07_produce)
        total_waste = sum(int(r["units_wasted"]) for r in s07_produce)
        print(f"S-07 Fresh Produce anomaly: {total_waste}/{total_recv} wasted ({total_waste/total_recv:.0%})")


if __name__ == "__main__":
    generate()
