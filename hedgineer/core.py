from typing import Any


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


def extract_attributes(audit_trail, attribute_priority):
    attributes = sorted(
        dict.fromkeys(map(lambda raw_fact: raw_fact[1], audit_trail)),
        key=lambda x: attribute_priority.get(x, float("inf")),
    )
    attribute_index = {v: i for i, v in enumerate(attributes)}

    return attributes, attribute_index


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


def get_pretty_table(table) -> str:
    s = [[str(e) for e in row] for row in table]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = "\t".join("{{:{0}}}".format(x) for x in lens)
    pretty_table = [fmt.format(*row) for row in s]
    return "\n".join(pretty_table)


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
