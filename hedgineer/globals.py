from datetime import datetime

from .utils import parse_date

ATTRIBUTE_PRIORITY = {
    "asset_class": 0,
    "ticker": 1,
    "name": 2,
}

AUDIT_TRAIL: list[tuple] = [
    (1, "ticker", "LENZ", parse_date("03/22/24")),
    (2, "market_cap", 549000, parse_date("05/23/24")),
    (1, "gics_sector", "healthcare", parse_date("01/01/24")),
    (1, "ticker", "GRPH", parse_date("01/01/24")),
    (1, "name", "Lenz Therapeutics, Inc", parse_date("03/22/24")),
    (2, "ticker", "V", parse_date("01/01/23")),
    (1, "gics_industry", "biotechnology", parse_date("01/01/24")),
    (2, "gics_sector", "technology", parse_date("01/01/23")),
    (1, "asset_class", "equity", parse_date("01/01/24")),
    (1, "name", "Graphite bio", parse_date("01/01/24")),
    (2, "gics_sector", "financials", parse_date("03/17/23")),
    (1, "market_cap", 400, parse_date("05/23/24")),
]

POSITIONS_TABLE = [
    (1, 100, datetime(2024, 2, 1)),
    (1, 105, datetime(2024, 2, 1)),
    (2, 150, datetime(2024, 2, 1)),
    (1, 120, datetime(2024, 3, 1)),
    (2, 140, datetime(2024, 3, 1)),
]
