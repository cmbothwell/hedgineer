import argparse
import os

from sqlalchemy import MetaData, create_engine

from .collect import filter_by_asset_class, generate_security_master, join_positions
from .globals import ATTRIBUTE_PRIORITY, POSITIONS_TABLE
from .io import (
    format_jp,
    format_sm,
    generate_audit_trail,
    read_audit_trail,
    read_sql,
    write_sql,
)
from .merge import merge_audit_trail_update

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="...")
    parser.add_argument("-g", "--generate", action="store_true")
    parser.add_argument("-m", "--merge", action="store_true")
    parser.add_argument("-f", "--filter", type=str)
    parser.add_argument("-p", "--positions", action="store_true")
    parser.add_argument("-s", "--sql", action="store_true")
    args = parser.parse_args()

    FILE_PATH = os.path.dirname(os.path.abspath(__file__))
    DATA_PATH = os.path.join(FILE_PATH, "..", "data")

    if args.generate:
        if args.merge:
            print("Cannot merge on generated data!")
        else:
            generate_audit_trail(os.path.join(DATA_PATH, "audit_trail_generated.csv"))
            audit_trail = read_audit_trail(
                os.path.join(DATA_PATH, "audit_trail_generated.csv")
            )
            sm = generate_security_master(audit_trail, ATTRIBUTE_PRIORITY)
            print(format_sm(sm, "Security Master (Generated)"))
    else:
        audit_trail = read_audit_trail(os.path.join(DATA_PATH, "audit_trail.csv"))
        sm = generate_security_master(audit_trail, ATTRIBUTE_PRIORITY)
        print(format_sm(sm, "Security Master"))

        if args.merge:
            audit_trail_update = read_audit_trail(
                os.path.join(DATA_PATH, "audit_trail_update.csv")
            )
            sm = merge_audit_trail_update(sm, audit_trail_update, ATTRIBUTE_PRIORITY)
            print(format_sm(sm, "Security Master after Merge"))

    if args.filter:
        if args.filter.strip().lower() == "none":
            sm = filter_by_asset_class(sm, None)
            print(format_sm(sm, f"Security Master (asset_class: {args.filter})"))
        else:
            sm = filter_by_asset_class(sm, args.filter)
            print(format_sm(sm, f"Security Master (asset_class: {args.filter})"))

    if args.positions:
        jp = join_positions(sm, POSITIONS_TABLE)
        print(format_jp(jp, "Consolidated Position Information"))

    if args.sql:
        engine = create_engine("sqlite:///:memory:", echo=True)
        metadata = MetaData()

        schema, metadata = write_sql(sm, engine, metadata, "security_master")
        sm = read_sql(schema, engine, metadata, "security_master")
