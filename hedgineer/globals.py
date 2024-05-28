from .utils import parse_date

ATTRIBUTE_PRIORITY = {
    "security_id": 0,
    "effective_start_date": 1,
    "effective_end_date": 2,
    "asset_class": 3,
    "ticker": 4,
    "name": 5,
}

TEST_AUDIT_TRAIL: list[tuple] = [
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

AUDIT_TRAIL: list[tuple] = [
    (1, "ticker", "LENZ", parse_date("03/22/24")),
    (3, "ticker", "ACME", parse_date("01/01/24")),
    (2, "market_cap", 549000, parse_date("05/23/24")),
    (1, "gics_sector", "healthcare", parse_date("01/01/24")),
    (1, "ticker", "GRPH", parse_date("01/01/24")),
    (1, "name", "Lenz Therapeutics, Inc", parse_date("03/22/24")),
    (2, "ticker", "V", parse_date("01/01/23")),
    (2, "asset_class", "fixed_income", parse_date("01/01/23")),
    (2, "interest_rate", 199, parse_date("01/01/23")),
    (1, "gics_industry", "biotechnology", parse_date("01/01/24")),
    (2, "gics_sector", "technology", parse_date("01/01/23")),
    (1, "asset_class", "equity", parse_date("01/01/24")),
    (1, "name", "Graphite bio", parse_date("01/01/24")),
    (2, "gics_sector", "financials", parse_date("03/17/23")),
    (1, "market_cap", 400, parse_date("05/23/24")),
]

AUDIT_TRAIL_UPDATE: list[tuple] = [
    (1, "market_cap", 100, parse_date("03/01/24")),
    (1, "gics_industry", "health sciences", parse_date("03/01/24")),
    (1, "market_cap", 90000, parse_date("05/26/24")),
    (2, "market_cap", 548000, parse_date("05/26/24")),
    (1, "new_key", 123, parse_date("05/26/24")),
    (2, "new_key", 456, parse_date("03/17/23")),
]

POSITIONS_TABLE = [
    (1, 100, parse_date("02/01/24")),
    (1, 105, parse_date("02/01/24")),
    (2, 150, parse_date("02/01/24")),
    (1, 120, parse_date("03/01/24")),
    (2, 140, parse_date("03/01/24")),
]
