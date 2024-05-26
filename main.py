from datetime import datetime
from functools import reduce
from operator import itemgetter
from typing import Any
from pprint import PrettyPrinter

pp = PrettyPrinter()

parse_date = lambda x: datetime.strptime(x, "%m/%d/%y")
format_date = lambda x: datetime.strftime(x, "%m/%d/%y")

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


def deeply_spread(dd):
    result = []

    for k, v in dd.items():
        if isinstance(v, dict):
            result.extend(map(lambda k_: (k, *k_), deeply_spread(v)))
        else:
            result.append((k, v))

    return result


def bucket_fact(bucket, fact):
    security_id, attribute_key, value, effective_date = fact

    bucket.setdefault(security_id, {effective_date: []}).setdefault(
        effective_date, []
    ).append((attribute_key, value))

    return bucket


def accumulate_fact(
    security_master__attributes__attribute_index: list[list[Any]], flat_fact: tuple
):
    security_master, attributes, attribute_index = (
        security_master__attributes__attribute_index
    )
    security_id, effective_date, facts = flat_fact
    new_security = len(security_master) == 0 or security_master[-1][0] != security_id

    if new_security:
        row = [
            security_id,
            effective_date,
            None,
            *map(lambda _: None, range(len(attributes))),
        ]
    else:
        row = list(security_master[-1])

    # Set date range
    row[1], row[2] = effective_date, None

    # Add new facts that diff from prior row
    for key, value in facts:
        row[3 + attribute_index[key]] = value

    # Don't forget to modify the last row's effective end date if needed
    if not new_security:
        security_master[-1] = tuple(
            (*security_master[-1][:2], effective_date, *security_master[-1][3:])
        )

    security_master.append(tuple(row))
    return (security_master, attributes, attribute_index)


def get_pretty_table(table) -> str:
    s = [[str(e) for e in row] for row in table]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = "\t".join("{{:{0}}}".format(x) for x in lens)
    pretty_table = [fmt.format(*row) for row in s]
    return "\n".join(pretty_table)


def join_position(security_master: list[tuple], position: tuple) -> tuple:
    security_id, quantity, date = position
    try:
        master_row = next(
            filter(
                lambda x: x[0] == security_id and x[1] <= date and x[2] > date,
                security_master,
            )
        )
    except StopIteration:
        return []

    return tuple((security_id, quantity, date, *master_row[3:]))


attributes = sorted(
    dict.fromkeys(map(lambda raw_fact: raw_fact[1], AUDIT_TRAIL)),
    key=lambda x: ATTRIBUTE_PRIORITY.get(x, float("inf")),
)
attribute_index = {v: i for i, v in enumerate(attributes)}

bucketed_facts: dict[str, dict[str, list]] = reduce(bucket_fact, AUDIT_TRAIL, {})
flat_facts: list[tuple] = deeply_spread(bucketed_facts)
sorted_flat_facts: list[tuple] = sorted(flat_facts, key=itemgetter(0, 1))

security_master, _, _ = reduce(
    accumulate_fact, sorted_flat_facts, ([], attributes, attribute_index)
)
formatted_security_master = [
    tuple(format_date(v) if isinstance(v, datetime) else v for v in t)
    for t in security_master
]
header = [["security_id", "effective_start_date", "effective_end_date", *attributes]]
print("Security Master")
print(get_pretty_table([*header, *formatted_security_master]), "\n")


joined_table = [
    join_position(security_master, position) for position in POSITIONS_TABLE
]
formatted_joined_table = [
    tuple(format_date(v) if isinstance(v, datetime) else v for v in t)
    for t in security_master
]
join_header = [["security_id", "quantity", "date", *attributes]]
print("Consolidated Position Information")
print(get_pretty_table([*join_header, *formatted_joined_table]), "\n")


# Scratchpad

# formatted_sorted_flat_facts = [
#     [format_date(val) if isinstance(val, datetime) else val for val in flat_fact]
#     for flat_fact in sorted_flat_facts
# ]

# flat_facts: list[tuple] = [
#     (security_id, date, facts)
#     for security_id, facts_by_date in bucketed_facts.items()
#     for date, facts in facts_by_date.items()
# ]


# def deeply_spread_(dd):
#     result_primitives = [(k, v) for k, v in dd.items() if not isinstance(v, dict)]
#     result_dicts = [
#         x
#         for k, v in dd.items()
#         if isinstance(v, dict)
#         for x in map(lambda k_: (k, *k_), deeply_spread(v))
#     ]
#     return [*result_dicts, *result_primitives]
