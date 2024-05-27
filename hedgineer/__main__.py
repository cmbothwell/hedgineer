from datetime import datetime
from functools import reduce
from operator import itemgetter
from pprint import PrettyPrinter

import numpy as np
import numpy.testing as npt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

from .globals import AUDIT_TRAIL, ATTRIBUTE_PRIORITY, POSITIONS_TABLE
from .core import (
    deeply_spread,
    bucket_fact,
    extract_attributes,
    accumulate_fact,
    join_position,
    get_pretty_table,
)
from .utils import format_date

pp = PrettyPrinter()


attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)

bucketed_facts: dict[str, dict[str, list]] = reduce(bucket_fact, AUDIT_TRAIL, {})
flat_facts: list[tuple] = deeply_spread(bucketed_facts)
sorted_flat_facts: list[tuple] = sorted(flat_facts, key=itemgetter(0, 1))

security_master, _, _ = reduce(
    accumulate_fact, sorted_flat_facts, ([], attributes, attribute_index)
)
formatted_security_master = [
    tuple(format_date(v) if isinstance(v, datetime) else v for v in t)
    for t in security_master
]
header = [["security_id", "effective_start_date", "effective_end_date", *attributes]]
print("Security Master")
print(get_pretty_table([*header, *formatted_security_master]), "\n")


joined_table = [
    join_position(security_master, position) for position in POSITIONS_TABLE
]
formatted_joined_table = [
    tuple(format_date(v) if isinstance(v, datetime) else v for v in t)
    for t in security_master
]
join_header = [["security_id", "quantity", "date", *attributes]]
print("Consolidated Position Information")
print(get_pretty_table([*join_header, *formatted_joined_table]), "\n")

# Pandas
# security_master_pd = pd.DataFrame(
#     security_master,
#     columns=["security_id", "effective_start_date", "effective_end_date", *attributes],
# )

# security_master_pd_copy = security_master_pd.copy(deep=True)
# assert security_master_pd.equals(security_master_pd_copy)

# PyArrow
# cols = list(zip(*security_master))
# security_ids = pa.array(cols[0], type=pa.int8())
# effective_start_dates = pa.array(cols[1], type=pa.date64())
# effective_end_dates = pa.array(cols[2], type=pa.date64())
# asset_classes = pa.array(cols[3], type=pa.string())
# tickers = pa.array(cols[4], type=pa.string())
# names = pa.array(cols[5], type=pa.string())
# market_cap = pa.array(cols[6], type=pa.float64())
# gics_sectors = pa.array(cols[7], type=pa.string())
# gics_industries = pa.array(cols[8], type=pa.string())

# arrow_table = pa.table(
#     [
#         security_ids,
#         effective_start_dates,
#         effective_end_dates,
#         asset_classes,
#         tickers,
#         names,
#         market_cap,
#         gics_sectors,
#         gics_industries,
#     ],
#     names=["security_id", "effective_start_date", "effective_end_date", *attributes],
# )


# pq.write_table(arrow_table, "security_master.parquet")
# reloaded_table = pq.read_table("security_master.parquet")

# csv.write_csv(reloaded_table, "security_master.csv")
# reloaded_table = csv.read_csv("security_master.csv")
