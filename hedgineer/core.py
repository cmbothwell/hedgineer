from functools import reduce
from operator import itemgetter
from typing import Any


def extract_attributes(audit_trail, attribute_priority):
    attributes = sorted(
        dict.fromkeys(map(lambda raw_fact: raw_fact[1], audit_trail)),
        key=lambda x: attribute_priority.get(x, float("inf")),
    )
    attribute_index = {v: i for i, v in enumerate(attributes)}

    return attributes, attribute_index


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


def bucket_facts(audit_trail):
    return reduce(bucket_fact, audit_trail, {})


def flatten_and_sort_facts(bucketed_facts):
    return sorted(deeply_spread(bucketed_facts), key=itemgetter(0, 1))


def generate_security_master_from_facts(sorted_flat_facts, attributes, attribute_index):
    security_master, _, _ = reduce(
        accumulate_fact, sorted_flat_facts, ([], attributes, attribute_index)
    )
    return security_master


def generate_security_master(audit_trail, attribute_priority):
    attributes, attribute_index = extract_attributes(audit_trail, attribute_priority)
    bucketed_facts = bucket_facts(audit_trail)
    sorted_flat_facts = flatten_and_sort_facts(bucketed_facts)

    header = ["security_id", "effective_start_date", "effective_end_date", *attributes]
    security_master = generate_security_master_from_facts(
        sorted_flat_facts, attributes, attribute_index
    )

    return (header, security_master)


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


def join_positions(
    attributes: list[str], security_master: list[tuple], positions_table: list[tuple]
):
    header = ["security_id", "quantity", "date", *attributes]
    joined_positions = [
        join_position(security_master, position) for position in positions_table
    ]
    return header, joined_positions
