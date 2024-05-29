import csv as pycsv
from datetime import date
from random import randint

import pyarrow as pa  # type: ignore
import pyarrow.csv as csv  # type: ignore
import pyarrow.parquet as pq  # type: ignore
from sqlalchemy import Column, Date, Float, Integer, String, Table, insert, select
from sqlalchemy.schema import CreateTable

from .types import AuditTrail, JoinedPositions, SecurityMaster
from .utils import format_date, parse_date, random_attribute_pair, random_day


def generate_audit_trail(path) -> None:
    data = [
        (
            randint(0, 15),
            *random_attribute_pair(),
            random_day(date(2023, 1, 1), date(2023, 12, 31)),
        )
        for _ in range(100)
    ]

    with open(path, mode="w+", newline="\n") as file:
        writer = pycsv.writer(
            file, quotechar='"', delimiter=",", quoting=pycsv.QUOTE_ALL
        )
        writer.writerows(data)


def read_audit_trail(path) -> AuditTrail:
    audit_trail = []

    with open(path, mode="r", newline="\n") as f:
        csv_reader = pycsv.reader(
            f,
            quotechar='"',
            delimiter=",",
            quoting=pycsv.QUOTE_ALL,
            skipinitialspace=True,
        )

        for row in csv_reader:
            audit_trail.append(row)

    return [
        tuple([int(row[0]), row[1], row[2], parse_date(row[3])]) for row in audit_trail
    ]


# https://stackoverflow.com/questions/13214809/pretty-print-2d-list
def get_pretty_table(table: list[tuple]):
    s = [[str(e) for e in row] for row in table]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = "\t".join("{{:{0}}}".format(x) for x in lens)
    pretty_table = [fmt.format(*row) for row in s]
    return "\n".join(pretty_table)


def format_sm(
    sm: SecurityMaster,
    title: str,
) -> str:
    table = [
        tuple(format_date(v) if isinstance(v, date) else v for v in t) for t in sm.data
    ]

    if len(table) == 0:
        return "No securities available\n"

    return title + "\n" + get_pretty_table([tuple(sm.header), *table]) + "\n"


def format_jp(
    jp: JoinedPositions,
    title: str,
) -> str:
    table = [
        tuple(format_date(v) if isinstance(v, date) else v for v in t) for t in jp.data
    ]

    if len(table) == 0:
        return "No positions available\n"

    return title + "\n" + get_pretty_table([tuple(jp.header), *table]) + "\n"


def parse_data_type(column):
    column_types = set(map(lambda x: type(x), filter(lambda x: x is not None, column)))

    if len(column_types) == 0:
        raise Exception(
            "Could not parse column for Arrow conversion: no elements provided"
        )

    if len(column_types) > 1:
        raise Exception(
            "Could not parse column for Arrow conversion: more than 1 data type present in column"
        )

    column_type = column_types.pop()

    if column_type is int:
        return pa.int64()
    elif column_type is float:
        return pa.float64()
    elif column_type is str:
        return pa.string()
    elif column_type is date:
        return pa.date32()

    raise Exception("Could not parse column for Arrow conversion: data type not found")


def to_arrow(sm: SecurityMaster) -> tuple[pa.Table, pa.Schema]:
    raw_columns = list(zip(*sm.data))
    data_types = map(parse_data_type, raw_columns)
    schema = pa.schema(list(zip(sm.header, data_types)))

    return pa.table(raw_columns, schema=schema), schema


def to_arrow_with_schema(data: list[tuple], schema):
    raw_columns = list(zip(*data))
    return pa.table(raw_columns, schema=schema), schema


def from_arrow(arrow_table) -> SecurityMaster:
    py_table = arrow_table.to_pylist()
    header = tuple(k for k in py_table[0].keys()) if len(py_table) > 0 else tuple()
    col_index = {v: i for i, v in enumerate(header)}
    data = [tuple(v for v in row.values()) for row in py_table]

    return SecurityMaster.from_tuple((list(header), data, col_index))


def to_pandas(sm: SecurityMaster):
    arrow_table, schema = to_arrow(sm)
    return arrow_table.to_pandas(), schema


def from_pandas(df, schema):
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    return from_arrow(arrow_table)


def write_parquet(sm, where):
    arrow_table, schema = to_arrow(sm)
    pq.write_table(arrow_table, where)

    return schema, where


def read_parquet(where, schema):
    arrow_table = pq.read_table(where, schema=schema)
    return from_arrow(arrow_table)


def write_csv(sm, where):
    arrow_table, schema = to_arrow(sm)
    convert_options = csv.ConvertOptions(
        column_types={field.name: field.type for field in schema},
        strings_can_be_null=True,
    )

    csv.write_csv(arrow_table, where)
    return convert_options, where


def read_csv(where, convert_options):
    arrow_table = csv.read_csv(where, convert_options=convert_options)
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
    sm: SecurityMaster,
    engine,
    metadata,
    table_name: str,
):
    arrow_table, schema = to_arrow(sm)
    columns = list(map(map_field_to_sql_column, schema))
    sql_table = Table(table_name, metadata, *columns)

    with engine.connect() as conn:
        conn.execute(CreateTable(sql_table, if_not_exists=True))
        conn.execute(insert(sql_table).values(arrow_table.to_pylist()))
        conn.commit()

    return schema, metadata


def read_sql(schema, engine, metadata, table_name: str):
    table = metadata.tables[table_name]
    with engine.connect() as conn:
        rows = list(conn.execute(select(table)))

    arrow_table, schema = to_arrow_with_schema(rows, schema)
    return from_arrow(arrow_table)
