from datetime import date

import pyarrow as pa
from sqlalchemy import MetaData, create_engine

from .collect import (
    extract_header,
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
    format_sm,
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
from .merge import merge_audit_trail_update

sm = generate_security_master(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
print(format_sm(sm, "Security Master"))

sm = merge_audit_trail_update(sm, AUDIT_TRAIL_UPDATE, ATTRIBUTE_PRIORITY)
print(format_sm(sm, "Security Master after Merge"))


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
    # Don't mutate the original
    filtered_sm_table = list(filter(lambda x: x[3] == asset_class, sm_table))
    return remove_empty_columns(sm_header, filtered_sm_table)


# jp_header, jp_table = join_positions(attributes, sm_table, POSITIONS_TABLE)


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


# print(
#     format_table(
#         "Security Master after Update",
#         sm_header,
#         sm_table,
#     )
# )

# sm_header_equities, sm_table_equities = filter_by_asset_class(
#     sm_header, sm_table, "equity"
# )
# sm_header_fi, sm_table_fi = filter_by_asset_class(sm_header, sm_table, "fixed_income")

# print(
#     format_table(
#         "Security Master (Equities)",
#         sm_header_equities,
#         sm_table_equities,
#     )
# )
# print(
#     format_table(
#         "Security Master (Fixed Income)",
#         sm_header_fi,
#         sm_table_fi,
#     )
# )

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
