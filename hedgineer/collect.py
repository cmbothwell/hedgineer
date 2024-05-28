from datetime import date
from functools import reduce
from operator import itemgetter

from .types import (
    AttributePair,
    AuditFact,
    AuditTrail,
    ColumnIndex,
    FlatFactSet,
    Header,
    SecurityMaster,
    SMData,
)
from .utils import deeply_spread, generate_none_tuple


def extract_header(
    audit_trail: AuditTrail, attribute_priority: dict[str, int]
) -> tuple[Header, ColumnIndex]:
    header = [
        "security_id",
        "effective_start_date",
        "effective_end_date",
        *list(dict.fromkeys(map(lambda raw_fact: raw_fact[1], audit_trail)).keys()),
    ]
    header = sorted(header, key=lambda x: (attribute_priority.get(x, float("inf")), x))
    col_index = {v: i for i, v in enumerate(header)}

    return header, col_index


def bucket_fact(bucket: dict[int, dict[date, list[AttributePair]]], fact: AuditFact):
    security_id, attribute_key, value, effective_date = fact
    bucket.setdefault(security_id, {effective_date: []}).setdefault(
        effective_date, []
    ).append((attribute_key, value))

    return bucket


def bucket_facts(audit_trail: AuditTrail) -> dict[int, dict[date, list[AttributePair]]]:
    return reduce(bucket_fact, audit_trail, {})


def flatten_and_sort_facts(
    bucketed_facts: dict[int, dict[date, list[AttributePair]]]
) -> list[FlatFactSet]:
    return sorted(deeply_spread(bucketed_facts), key=itemgetter(0, 1))


def generate_sorted_flat_facts(audit_trail: AuditTrail):
    bucketed_facts = bucket_facts(audit_trail)
    return flatten_and_sort_facts(bucketed_facts)


def diff_row(prior_row: tuple, col_index: ColumnIndex, flat_fact: FlatFactSet):
    new_row = list(prior_row)
    security_id, effective_date, kv_pairs = flat_fact

    # Set id & date range
    (
        new_row[col_index["security_id"]],
        new_row[col_index["effective_start_date"]],
    ) = (security_id, effective_date)

    # Add new kv_pairs that diff from prior row
    for key, value in kv_pairs:
        new_row[col_index[key]] = value

    return tuple(new_row)


def accumulate_fact(
    data__col_index: tuple[SMData, ColumnIndex], flat_fact: FlatFactSet
):
    data, col_index = data__col_index
    security_id, effective_date, _ = flat_fact

    is_new_security = (
        len(data) == 0 or data[-1][col_index["security_id"]] != security_id
    )
    prior_row = generate_none_tuple(len(col_index)) if is_new_security else data[-1]
    new_row = diff_row(prior_row, col_index, flat_fact)

    # Modify the last row's end date
    if not is_new_security:
        index = col_index["effective_end_date"]
        data[-1] = tuple((*data[-1][:index], effective_date, *data[-1][index + 1 :]))

    # Now we can append the new row
    data.append(new_row)
    return (data, col_index)


def generate_data_from_facts(
    sorted_flat_facts: list[FlatFactSet],
    col_index: ColumnIndex,
) -> SMData:
    sm_data, _ = reduce(accumulate_fact, sorted_flat_facts, ([], col_index))
    return sm_data


def generate_security_master(
    audit_trail: AuditTrail, attribute_priority: dict[str, int]
) -> SecurityMaster:
    header, col_index = extract_header(audit_trail, attribute_priority)
    sorted_flat_facts = generate_sorted_flat_facts(audit_trail)
    data = generate_data_from_facts(sorted_flat_facts, col_index)

    return SecurityMaster.from_tuple((header, data, col_index))


# TODO Typing
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
