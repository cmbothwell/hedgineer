from typing import Any

from .collect import diff_row, extract_header, generate_sorted_flat_facts
from .globals import ATTRIBUTE_PRIORITY
from .io import format_sm
from .types import AttributePair, AuditTrail, FlatFactSet, SecurityMaster
from .utils import generate_none_tuple, replace_at_index


def expand_attributes(
    sm: SecurityMaster,
    audit_trail_update: AuditTrail,
    attribute_priority: dict[str, int],
):
    header_from_update, _ = extract_header(audit_trail_update, attribute_priority)
    new_columns = list(filter(lambda x: x not in sm.header, header_from_update))

    merged_header = sorted(
        sm.header + new_columns,
        key=lambda x: (attribute_priority.get(x, float("inf")), x),
    )
    merged_column_index = {v: i for i, v in enumerate(merged_header)}

    data_length = len(sm.data)
    old_data_as_cols = list(zip(*sm.data))
    merged_data_as_cols = [
        *map(lambda _: generate_none_tuple(data_length), range(len(merged_header))),
    ]

    for value in sm.header:
        merged_data_as_cols[merged_column_index[value]] = old_data_as_cols[
            sm.col_index[value]
        ]

    return SecurityMaster.from_tuple(
        (merged_header, list(zip(*merged_data_as_cols)), merged_column_index)
    )


def get_value_diffs(
    sm: SecurityMaster, current_row: tuple, kv_pairs: list[AttributePair]
):
    return [
        (
            sm.col_index[attribute],
            current_row[sm.col_index[attribute]],
            value,
        )
        for attribute, value in kv_pairs
    ]


def cascade_new_values(
    sm: SecurityMaster,
    security_id: int,
    starting_index: int,
    value_diffs: list[tuple[int, Any, Any]],
):
    for i in range(starting_index, len(sm.data)):
        row = sm.data[i]
        security_id_ = row[sm.col_index["security_id"]]

        if security_id_ != security_id:
            break

        for index, old_value, new_value in value_diffs:
            if row[index] is None or row[index] == old_value:
                row = replace_at_index(row, index, new_value)

        sm.data[i] = row

    return sm


def insert_new_security(sm: SecurityMaster, flat_fact: FlatFactSet) -> SecurityMaster:
    security_id, _, _ = flat_fact
    empty_row = generate_none_tuple(len(sm.col_index))
    new_row = diff_row(empty_row, sm.col_index, flat_fact)

    if security_id > max(row[sm.col_index["security_id"]] for row in sm.data):
        sm.data.append(new_row)
    else:
        for i, row in enumerate(sm.data):
            s_id = row[sm.col_index["security_id"]]
            insertion_index = i

            if s_id > security_id:
                break

        sm.data.insert(insertion_index, new_row)

    return sm


def insert_before(
    sm: SecurityMaster, row_to_insert_before: tuple, flat_fact: FlatFactSet
):
    empty_row = generate_none_tuple(len(sm.col_index))
    new_row = diff_row(empty_row, sm.col_index, flat_fact)

    # set end date to start date of next row
    new_row = replace_at_index(
        new_row,
        sm.col_index["effective_end_date"],
        row_to_insert_before[sm.col_index["effective_start_date"]],
    )

    new_row_index = sm.data.index(row_to_insert_before)
    sm.data.insert(new_row_index, new_row)

    value_diffs = get_value_diffs(sm, new_row, flat_fact[2])
    return cascade_new_values(sm, flat_fact[0], new_row_index, value_diffs)


def insert_after(
    sm: SecurityMaster, row_to_insert_after: tuple, flat_fact: FlatFactSet
):
    new_row = diff_row(row_to_insert_after, sm.col_index, flat_fact)

    # Replace old end date
    prior_row_index = sm.data.index(row_to_insert_after)
    sm.data[prior_row_index] = replace_at_index(
        row_to_insert_after, sm.col_index["effective_end_date"], flat_fact[1]
    )

    # Insert new row
    sm.data.insert(prior_row_index + 1, new_row)

    return sm


def merge_into_row(
    sm: SecurityMaster,
    row_to_merge_into: tuple,
    flat_fact: FlatFactSet,
):
    row_to_merge_index = sm.data.index(row_to_merge_into)
    value_diffs = get_value_diffs(sm, row_to_merge_into, flat_fact[2])
    return cascade_new_values(sm, flat_fact[0], row_to_merge_index, value_diffs)


def split_row(sm: SecurityMaster, row_to_split: tuple, flat_fact: FlatFactSet):
    split_index = sm.data.index(row_to_split)
    new_row = row_to_split
    next_row = sm.data[split_index + 1]

    # Update end date for prior entry
    sm.data[split_index] = replace_at_index(
        row_to_split, sm.col_index["effective_end_date"], flat_fact[1]
    )

    # Update start date of the new row
    new_row = replace_at_index(
        new_row, sm.col_index["effective_start_date"], flat_fact[1]
    )

    # Update end date of the new row
    new_row = replace_at_index(
        new_row,
        sm.col_index["effective_end_date"],
        next_row[sm.col_index["effective_start_date"]],
    )

    # Insert the new row
    sm.data.insert(split_index + 1, new_row)

    value_diffs = get_value_diffs(sm, new_row, flat_fact[2])
    return cascade_new_values(sm, flat_fact[0], split_index + 1, value_diffs)


def merge_flat_fact(
    sm: SecurityMaster,
    flat_fact: FlatFactSet,
) -> SecurityMaster:
    security_id, d, _ = flat_fact
    security_rows = list(filter(lambda x: x[0] == security_id, sm.data))

    if len(security_rows) == 0:
        return insert_new_security(sm, flat_fact)
    elif d < security_rows[0][1]:
        return insert_before(
            sm,
            security_rows[0],
            flat_fact,
        )
    elif d > security_rows[-1][1]:
        return insert_after(
            sm,
            security_rows[-1],
            flat_fact,
        )
    elif any(map(lambda x: x[1] == d, security_rows)):
        row_to_merge_into = next(row for row in security_rows if row[1] == d)
        return merge_into_row(
            sm,
            row_to_merge_into,
            flat_fact,
        )
    else:
        row_to_split = next(row for row in security_rows if row[1] <= d < row[2])
        return split_row(sm, row_to_split, flat_fact)


def merge_audit_trail_update(
    sm: SecurityMaster,
    audit_trail_update: AuditTrail,
    attribute_priority: dict[str, int],
):
    sm = expand_attributes(
        sm,
        audit_trail_update,
        attribute_priority,
    )

    for flat_fact in generate_sorted_flat_facts(audit_trail_update):
        sm = merge_flat_fact(
            sm,
            flat_fact,
        )

    return sm
