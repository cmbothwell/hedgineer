from datetime import date

import pyarrow as pa
from sqlalchemy import MetaData, create_engine

from .collect import filter_by_asset_class, generate_security_master, join_positions
from .globals import (
    ATTRIBUTE_PRIORITY,
    AUDIT_TRAIL,
    AUDIT_TRAIL_UPDATE,
    POSITIONS_TABLE,
)
from .io import (
    format_jp,
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

sm_equity = filter_by_asset_class(sm, "equity")
print(format_sm(sm_equity, "Security Master (Equities)"))

sm_fi = filter_by_asset_class(sm, "fixed_income")
print(format_sm(sm_fi, "Security Master (Fixed Income)"))

sm_catch_all = filter_by_asset_class(sm, None)
print(format_sm(sm_catch_all, "Security Master (Other)"))

jp = join_positions(sm, POSITIONS_TABLE)
print(format_jp(jp, "Consolidated Position Information"))


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
