from datetime import date

from sqlalchemy import MetaData, create_engine

from .collect import filter_by_asset_class, generate_security_master, join_positions
from .globals import (
    ATTRIBUTE_PRIORITY,
    AUDIT_TRAIL,
    AUDIT_TRAIL_UPDATE,
    POSITIONS_TABLE,
)
from .io import format_jp, format_sm, read_sql, write_sql
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

# engine = create_engine("sqlite:///:memory:", echo=True)
# metadata = MetaData()

# schema, metadata = write_sql(sm, engine, metadata, "security_master")
# sm = read_sql(schema, engine, metadata, "security_master")
