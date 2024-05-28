from datetime import date

import pyarrow as pa
from sqlalchemy import MetaData, create_engine

from .collect import (
    extract_attributes,
    generate_security_master,
    generate_sorted_flat_facts,
    join_positions,
)
from .globals import (
    ATTRIBUTE_PRIORITY,
    AUDIT_TRAIL,
    AUDIT_TRAIL_UPDATE,
    POSITIONS_TABLE,
)
from .io import (
    format_table,
    from_arrow,
    from_pandas,
    read_csv,
    read_parquet,
    read_sql,
    to_arrow,
    to_pandas,
    write_csv,
    write_parquet,
    write_sql,
)
from .merge import merge_flat_fact

attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
sm_header, sm_table = generate_security_master(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
jp_header, jp_table = join_positions(attributes, sm_table, POSITIONS_TABLE)

print(
    format_table(
        "Security Master",
        sm_header,
        sm_table,
    )
)

# arrow_table, schema = to_arrow(sm_header, sm_table)
# converted_header, converted_table = from_arrow(arrow_table)

# df, schema = to_pandas(sm_header, sm_table)
# converted_header, converted_table = from_pandas(df, schema)

# filename, schema = write_parquet("./data/sm_table.parquet", sm_header, sm_table)
# converted_header, converted_table = read_parquet(filename, schema)

# filename, convert_options = write_csv("./data/sm_table.csv", sm_header, sm_table)
# converted_header, converted_table = read_csv(filename, convert_options)


# print(
#     format_table(
#         "Security Master",
#         sm_header,
#         sm_table,
#     )
# )


def merge_audit_trail_update(sm_header, sm_table, audit_trail_update, attribute_index):
    flat_facts_to_merge = generate_sorted_flat_facts(audit_trail_update)
    for flat_fact in flat_facts_to_merge:
        sm_header, sm_table = merge_flat_fact(
            sm_header, sm_table, flat_fact, attributes, attribute_index
        )

    return sm_header, sm_table


def remove_empty_columns(sm_header, sm_table):
    column_empty_map = list(
        map(lambda col: all(val is None for val in col), zip(*sm_table))
    )
    sm_header = [v for i, v in enumerate(sm_header) if not column_empty_map[i]]
    sm_table = [
        tuple(v for i, v in enumerate(t) if not column_empty_map[i]) for t in sm_table
    ]
    return sm_header, sm_table


def filter_by_asset_class(sm_header, sm_table, asset_class):
    sm_table = list(filter(lambda x: x[3] == asset_class, sm_table))
    return remove_empty_columns(sm_header, sm_table)


sm_header, sm_table = merge_audit_trail_update(
    sm_header, sm_table, AUDIT_TRAIL_UPDATE, attribute_index
)
print(
    format_table(
        "Security Master",
        sm_header,
        sm_table,
    )
)

sm_header, sm_table = filter_by_asset_class(sm_header, sm_table, "equity")
print(
    format_table(
        "Security Master",
        sm_header,
        sm_table,
    )
)

# print(
#     format_table(
#         "Consolidated Position Information",
#         jp_header,
#         jp_table,
#     )
# )

# engine = create_engine("sqlite:///:memory:", echo=True)
# metadata = MetaData()

# metadata, schema = write_sql(
#     engine, metadata, "security_master", sm_header, security_master
# )
# arrow_table, schema = read_sql(engine, metadata, "security_master", schema)
