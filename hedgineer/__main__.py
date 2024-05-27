from .core import (
    extract_attributes,
    generate_security_master,
    join_positions,
)
from .io import format_table, to_arrow, write_parquet, write_csv
from .globals import ATTRIBUTE_PRIORITY, AUDIT_TRAIL, POSITIONS_TABLE

attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
sm_header, security_master = generate_security_master(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
jp_header, joined_positions = join_positions(
    attributes, security_master, POSITIONS_TABLE
)

arrow_table = to_arrow(
    sm_header,
    security_master,
)

write_parquet("./data/security_master.parquet", sm_header, security_master)
write_csv("./data/security_master.csv", sm_header, security_master)

print(
    format_table(
        "Security Master",
        sm_header,
        security_master,
    )
)

print(
    format_table(
        "Consolidated Position Information",
        jp_header,
        joined_positions,
    )
)
