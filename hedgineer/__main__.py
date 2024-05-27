import pyarrow as pa
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
sm_header, sm_table = generate_security_master(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
jp_header, joined_positions = join_positions(attributes, sm_table, POSITIONS_TABLE)

# arrow_table, schema = to_arrow(sm_header, sm_table)
# converted_header, converted_table = from_arrow(arrow_table)

# df, schema = to_pandas(sm_header, sm_table)
# converted_header, converted_table = from_pandas(df, schema)

# filename, schema = write_parquet("./data/sm_table.parquet", sm_header, sm_table)
# converted_header, converted_table = read_parquet(filename, schema)

# filename, convert_options = write_csv("./data/sm_table.csv", sm_header, sm_table)
# converted_header, converted_table = read_csv(filename, convert_options)

print(
    format_table(
        "Security Master",
        sm_header,
        sm_table,
    )
)

print(
    format_table(
        "Consolidated Position Information",
        jp_header,
        joined_positions,
    )
)

# engine = create_engine("sqlite:///:memory:", echo=True)
# metadata = MetaData()

# metadata, schema = write_sql(
#     engine, metadata, "security_master", sm_header, security_master
# )
# arrow_table, schema = read_sql(engine, metadata, "security_master", schema)
