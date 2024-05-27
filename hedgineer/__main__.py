from sqlalchemy import MetaData, create_engine

from .core import extract_attributes, generate_security_master, join_positions
from .globals import ATTRIBUTE_PRIORITY, AUDIT_TRAIL, POSITIONS_TABLE
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

attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
sm_header, security_master = generate_security_master(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
jp_header, joined_positions = join_positions(
    attributes, security_master, POSITIONS_TABLE
)

# arrow_table, schema = to_arrow(sm_header, security_master)
# converted_header, converted_table = from_arrow(arrow_table)
# assert sm_header == converted_header
# assert security_master == converted_table


# df, schema = to_pandas(sm_header, security_master)
# converted_header, converted_table = from_pandas(df, schema)
# assert sm_header == converted_header
# assert security_master == converted_table


# schema = write_parquet("./data/security_master.parquet", sm_header, security_master)
# converted_header, converted_table = read_parquet(
#     "./data/security_master.parquet", schema
# )
# assert sm_header == converted_header
# assert security_master == converted_table


# convert_options = write_csv("./data/security_master.csv", sm_header, security_master)
# converted_header, converted_table = read_csv(
#     "./data/security_master.csv", convert_options
# )
# assert sm_header == converted_header
# assert security_master == converted_table

# print(
#     format_table(
#         "Security Master",
#         sm_header,
#         security_master,
#     )
# )

# print(
#     format_table(
#         "Security Master",
#         converted_header,
#         converted_table,
#     )
# )

# print(
#     format_table(
#         "Consolidated Position Information",
#         jp_header,
#         joined_positions,
#     )
# )


# engine = create_engine("sqlite:///:memory:", echo=True)
# metadata = MetaData()

# metadata, schema = write_sql(
#     engine, metadata, "security_master", sm_header, security_master
# )
# arrow_table, schema = read_sql(engine, metadata, "security_master", schema)
