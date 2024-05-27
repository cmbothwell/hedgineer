from .core import (
    extract_attributes,
    format_table,
    generate_security_master,
    join_positions,
)
from .globals import ATTRIBUTE_PRIORITY, AUDIT_TRAIL, POSITIONS_TABLE

attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
security_master = generate_security_master(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
joined_positions = join_positions(security_master, POSITIONS_TABLE)

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
        joined_positions,
    )
)
