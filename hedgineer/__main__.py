import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

from .core import (
    bucket_facts,
    extract_attributes,
    flatten_and_sort_facts,
    format_table,
    generate_security_master,
    join_position,
)
from .globals import ATTRIBUTE_PRIORITY, AUDIT_TRAIL, POSITIONS_TABLE

attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
bucketed_facts = bucket_facts(AUDIT_TRAIL)
sorted_flat_facts = flatten_and_sort_facts(bucketed_facts)
security_master = generate_security_master(
    sorted_flat_facts, attributes, attribute_index
)
joined_table = [
    join_position(security_master, position) for position in POSITIONS_TABLE
]

print(
    format_table(
        "Security Master",
        ["security_id", "effective_start_date", "effective_end_date", *attributes],
        security_master,
    )
)

print(
    format_table(
        "Consolidated Position Information",
        ["security_id", "quantity", "date", *attributes],
        joined_table,
    )
)
