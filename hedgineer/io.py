from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

from .utils import format_date


def get_pretty_table(table) -> str:
    s = [[str(e) for e in row] for row in table]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = "\t".join("{{:{0}}}".format(x) for x in lens)
    pretty_table = [fmt.format(*row) for row in s]
    return "\n".join(pretty_table)


def format_table(title, header, table) -> str:
    table = [
        tuple(format_date(v) if isinstance(v, datetime) else v for v in t)
        for t in table
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
    if column_type is datetime:
        return pa.date64()

    raise Exception("Could not parse column for Arrow conversion: data type not found")


def to_arrow(
    column_names: list[str],
    table: list[tuple],
):
    if len(table) == 0:
        return None

    raw_columns = list(zip(*table))
    data_types = [parse_data_type(raw_column) for raw_column in raw_columns]
    arrow_columns = list(
        map(lambda x: pa.array(x[0], type=x[1]), zip(raw_columns, data_types))
    )

    return pa.table(arrow_columns, names=column_names)


def write_parquet(filename: str, column_names: list[str], table: list[tuple]):
    arrow_table = to_arrow(column_names, table)
    pq.write_table(arrow_table, filename)

    return None


def write_csv(filename: str, column_names: list[str], table: list[tuple]):
    arrow_table = to_arrow(column_names, table)
    csv.write_csv(arrow_table, filename)


# Pandas
# security_master_pd = pd.DataFrame(
#     security_master,
#     columns=["security_id", "effective_start_date", "effective_end_date", *attributes],
# )

# security_master_pd_copy = security_master_pd.copy(deep=True)
# assert security_master_pd.equals(security_master_pd_copy)

# pq.write_table(arrow_table, "security_master.parquet")
# reloaded_table = pq.read_table("security_master.parquet")

# csv.write_csv(reloaded_table, "security_master.csv")
# reloaded_table = csv.read_csv("security_master.csv")
