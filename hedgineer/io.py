from datetime import date

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
from sqlalchemy import Column, Date, Float, Integer, String, Table, insert, select
from sqlalchemy.schema import CreateTable

from .utils import format_date


def get_pretty_table(table) -> str:
    s = [[str(e) for e in row] for row in table]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = "\t".join("{{:{0}}}".format(x) for x in lens)
    pretty_table = [fmt.format(*row) for row in s]
    return "\n".join(pretty_table)


def format_table(title, header, table) -> str:
    table = [
        tuple(format_date(v) if isinstance(v, date) else v for v in t) for t in table
    ]
    return title + "\n" + get_pretty_table([header, *table]) + "\n"


def parse_data_type(column):
    column_types = set(
        map(lambda x: type(x), (filter(lambda x: x is not None, column)))
    )

    if len(column_types) != 1:
        raise Exception(
            "Could not parse column for Arrow conversion: more than 1 data type present in column"
        )

    column_type = column_types.pop()

    if column_type is int:
        return pa.int64()
    if column_type is float:
        return pa.float64()
    if column_type is str:
        return pa.string()
    if column_type is date:
        return pa.date32()

    raise Exception("Could not parse column for Arrow conversion: data type not found")


def to_arrow(column_names: list[str], table: list[tuple]):
    raw_columns = list(zip(*table))
    data_types = map(parse_data_type, raw_columns)
    schema = pa.schema(list(zip(column_names, data_types)))

    return pa.table(raw_columns, schema=schema), schema


def to_arrow_with_schema(table: list[tuple], schema):
    raw_columns = list(zip(*table))
    return pa.table(raw_columns, schema=schema), schema


def from_arrow(arrow_table):
    py_table = arrow_table.to_pylist()
    header = [k for k in py_table[0].keys()] if len(py_table) > 0 else []
    table = [tuple(v for v in row.values()) for row in py_table]

    return header, table


def to_pandas(column_names: list[str], table: list[tuple]):
    arrow_table, schema = to_arrow(column_names, table)
    return arrow_table.to_pandas(), schema


def from_pandas(df, schema):
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    return from_arrow(arrow_table)


def write_parquet(filename: str, column_names: list[str], table: list[tuple]):
    arrow_table, schema = to_arrow(column_names, table)
    pq.write_table(arrow_table, filename)

    return schema


def read_parquet(filename: str, schema):
    arrow_table = pq.read_table(filename, schema=schema)
    return from_arrow(arrow_table)


def write_csv(filename: str, column_names: list[str], table: list[tuple]):
    arrow_table, schema = to_arrow(column_names, table)
    convert_options = csv.ConvertOptions(
        column_types={field.name: field.type for field in schema},
        strings_can_be_null=True,
    )

    csv.write_csv(arrow_table, filename)
    return convert_options


def read_csv(filename: str, convert_options):
    arrow_table = csv.read_csv(filename, convert_options=convert_options)
    return from_arrow(arrow_table)


def map_field_to_sql_column(field):
    if field.type == pa.int64():
        column_type = Integer
    elif field.type == pa.float64():
        column_type = Float
    elif field.type == pa.string():
        column_type = String
    elif field.type == pa.date32():
        column_type = Date
    else:
        raise Exception("Could not determine column type from Arrow schema")

    if field.name == "security_id" or field.name == "effective_start_date":
        return Column(field.name, column_type, primary_key=True)
    else:
        return Column(field.name, column_type, nullable=True)


def write_sql(
    engine,
    metadata,
    table_name: str,
    column_names: list[str],
    table: list[tuple],
):
    arrow_table, schema = to_arrow(column_names, table)
    columns = list(map(map_field_to_sql_column, schema))
    sql_table = Table(table_name, metadata, *columns)

    with engine.connect() as conn:
        conn.execute(CreateTable(sql_table, if_not_exists=True))
        conn.execute(insert(sql_table).values(arrow_table.to_pylist()))
        conn.commit()

    return metadata, schema


def read_sql(engine, metadata, table_name, schema):
    table = metadata.tables[table_name]

    with engine.connect() as conn:
        rows = list(conn.execute(select(table)))

    return to_arrow_with_schema(rows, schema)
