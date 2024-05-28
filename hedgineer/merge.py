from .collect import diff_row, generate_empty_row
from .utils import replace_at_index


def get_value_diffs(current_row, facts, attribute_index):
    return [
        (
            attribute_index[attribute] + 3,
            current_row[attribute_index[attribute] + 3],
            value,
        )
        for attribute, value in facts
    ]


def cascade_new_values(table, security_id, starting_index, value_diffs):
    for i in range(starting_index, len(table)):
        row = table[i]
        security_id_, *_ = row

        if security_id_ != security_id:
            break

        for index, old_value, new_value in value_diffs:
            if row[index] is None or row[index] == old_value:
                row = replace_at_index(row, index, new_value)

        table[i] = row


def insert_new_security(sm_table, flat_fact, attributes, attribute_index):
    security_id, d, kv_pairs = flat_fact
    empty_row = generate_empty_row(len(attributes) + 3)
    new_row = diff_row(
        empty_row,
        security_id,
        d,
        attribute_index,
        kv_pairs,
    )

    for i, row in enumerate(sm_table):
        s_id, *_ = row
        insertion_index = i

        if s_id > security_id:
            break

    sm_table.insert(insertion_index, new_row)


def insert_before(
    sm_table, row_to_insert_before, flat_fact, attributes, attribute_index
):
    security_id, d, kv_pairs = flat_fact
    empty_row = generate_empty_row(len(attributes) + 3)
    new_row = diff_row(
        empty_row,
        security_id,
        d,
        attribute_index,
        kv_pairs,
    )

    new_index = sm_table.index(row_to_insert_before)
    new_row = replace_at_index(new_row, 2, row_to_insert_before[1])
    sm_table.insert(new_index, new_row)

    value_diffs = get_value_diffs(new_row, kv_pairs, attribute_index)
    cascade_new_values(sm_table, security_id, new_index, value_diffs)


def insert_after(sm_table, row_to_insert_after, flat_fact, _, attribute_index):
    security_id, d, kv_pairs = flat_fact
    new_row = diff_row(
        row_to_insert_after,
        security_id,
        d,
        attribute_index,
        kv_pairs,
    )

    prior_index = sm_table.index(row_to_insert_after)
    sm_table[prior_index] = replace_at_index(  # Replace old end date
        row_to_insert_after, 2, d
    )

    # Insert new row
    sm_table.insert(prior_index + 1, new_row)


def merge_into_row(sm_table, row_to_merge_into, flat_fact, _, attribute_index):
    security_id, _, kv_pairs = flat_fact
    row_to_merge_index = sm_table.index(row_to_merge_into)

    value_diffs = get_value_diffs(row_to_merge_into, kv_pairs, attribute_index)
    cascade_new_values(sm_table, security_id, row_to_merge_index, value_diffs)


def split_row(sm_table, row_to_split, flat_fact, _, attribute_index):
    security_id, d, kv_pairs = flat_fact
    split_index = sm_table.index(row_to_split)

    new_row = row_to_split
    next_row = sm_table[split_index + 1]

    # Update end date for prior entry
    sm_table[split_index] = replace_at_index(row_to_split, 2, d)

    new_row = replace_at_index(new_row, 1, d)  # Update start date
    new_row = replace_at_index(new_row, 2, next_row[1])  # Update end date
    sm_table.insert(split_index + 1, new_row)  # Insert the new row

    value_diffs = get_value_diffs(new_row, kv_pairs, attribute_index)
    cascade_new_values(sm_table, security_id, split_index + 1, value_diffs)


def merge_flat_fact(sm_table, flat_fact, attributes, attribute_index):
    security_id, d, _ = flat_fact
    security_rows = list(filter(lambda x: x[0] == security_id, sm_table))

    if len(security_rows) == 0:
        # print("Empty case")
        insert_new_security(sm_table, flat_fact, attributes, attribute_index)
    elif d < security_rows[0][1]:
        # print("Before case")
        insert_before(
            sm_table, security_rows[0], flat_fact, attributes, attribute_index
        )
    elif d > security_rows[-1][1]:
        # print("After case")
        insert_after(
            sm_table, security_rows[-1], flat_fact, attributes, attribute_index
        )
    elif any(map(lambda x: x[1] == d, security_rows)):
        # print("Merge case")
        row_to_merge_into = next(row for row in security_rows if row[1] == d)
        merge_into_row(
            sm_table, row_to_merge_into, flat_fact, attributes, attribute_index
        )
    else:
        # print("Split case")
        row_to_split = next(row for row in security_rows if row[1] <= d < row[2])
        split_row(sm_table, row_to_split, flat_fact, attributes, attribute_index)
