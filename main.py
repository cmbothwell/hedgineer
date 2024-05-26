from collections import namedtuple
from datetime import datetime
from functools import reduce
from operator import attrgetter
from typing import Any

parse_date = lambda x: datetime.strptime(x, "%m/%d/%y")
format_date = lambda x: datetime.strftime(x, "%m/%d/%y")

RawFact = namedtuple(
    "RawFact", ["security_id", "attribute_key", "value", "effective_date"]
)
Fact = namedtuple("Fact", ["attribute_key", "value"])
FlatFact = namedtuple("FlatFact", ["security_id", "effective_date", "facts"])


def bucket_fact(bucket, f: RawFact):
    bucket.setdefault(f.security_id, {f.effective_date: []}).setdefault(
        f.effective_date, []
    ).append((f.attribute_key, f.value))

    return bucket


def accumulate_flat_fact(security_master: list[list[Any]], flat_fact: FlatFact):
    security_id, effective_date, facts = flat_fact

    first_row = len(security_master) == 0
    new_security = not first_row and security_master[-1][0] != security_id

    if first_row or new_security:
        row = [
            security_id,
            effective_date,
            None,
            *map(lambda _: None, range(len(attributes))),
        ]
    else:
        row = security_master[-1].copy()

    # Set date range
    row[1], row[2] = effective_date, None

    # Add new facts that diff from prior row
    for key, value in facts:
        row[3 + attribute_index[key]] = value

    # Don't forget to modify the last row's effective end date if needed
    if not first_row and not new_security:
        security_master[-1][2] = effective_date

    security_master.append(row)
    return security_master


def get_pretty_table(table) -> str:
    s = [[str(e) for e in row] for row in table]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = "\t".join("{{:{0}}}".format(x) for x in lens)
    pretty_table = [fmt.format(*row) for row in s]
    return "\n".join(pretty_table)


audit_trail: list[RawFact] = [
    RawFact(1, "ticker", "LENZ", parse_date("03/22/24")),
    RawFact(2, "market_cap", 549000, parse_date("05/23/24")),
    RawFact(1, "gics_sector", "healthcare", parse_date("01/01/24")),
    RawFact(1, "ticker", "GRPH", parse_date("01/01/24")),
    RawFact(1, "name", "Lenz Therapeutics, Inc", parse_date("03/22/24")),
    RawFact(2, "ticker", "V", parse_date("01/01/23")),
    RawFact(1, "gics_industry", "biotechnology", parse_date("01/01/24")),
    RawFact(2, "gics_sector", "technology", parse_date("01/01/23")),
    RawFact(1, "asset_class", "equity", parse_date("01/01/24")),
    RawFact(1, "name", "Graphite bio", parse_date("01/01/24")),
    RawFact(2, "gics_sector", "financials", parse_date("03/17/23")),
    RawFact(1, "market_cap", 400, parse_date("05/23/24")),
]


attribute_priority = {
    "asset_class": 0,
    "ticker": 1,
    "name": 2,
}
attributes = sorted(
    dict.fromkeys(map(lambda raw_fact: raw_fact.attribute_key, audit_trail)),
    key=lambda x: attribute_priority.get(x, float("inf")),
)
attribute_index = {v: i for i, v in enumerate(attributes)}


bucketed_facts: dict[str, dict[str, list[Fact]]] = reduce(bucket_fact, audit_trail, {})
flat_facts: list[FlatFact] = [
    FlatFact(security_id, date, facts)
    for security_id, facts_by_date in bucketed_facts.items()
    for date, facts in facts_by_date.items()
]
sorted_flat_facts: list[FlatFact] = sorted(
    flat_facts, key=attrgetter("security_id", "effective_date")
)
formatted_sorted_flat_facts = [
    [format_date(val) if isinstance(val, datetime) else val for val in flat_fact]
    for flat_fact in sorted_flat_facts
]
security_master = [
    ["security_id", "effective_start_date", "effective_end_date", *attributes]
] + reduce(accumulate_flat_fact, formatted_sorted_flat_facts, [])


print(get_pretty_table(security_master))
