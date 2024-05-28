from datetime import date

import pyarrow as pa
from pytest import fixture, raises
from sqlalchemy import MetaData, create_engine

from hedgineer.collect import (
    extract_header,
    generate_security_master,
    join_positions,
)
from hedgineer.globals import ATTRIBUTE_PRIORITY, TEST_AUDIT_TRAIL, POSITIONS_TABLE
from hedgineer.io import (
    from_arrow,
    from_pandas,
    parse_data_type,
    read_csv,
    read_parquet,
    read_sql,
    to_arrow,
    to_pandas,
    write_csv,
    write_parquet,
    write_sql,
)


@fixture
def audit_trail():
    return TEST_AUDIT_TRAIL


@fixture
def attribute_priority():
    return ATTRIBUTE_PRIORITY


@fixture
def positions_table():
    return POSITIONS_TABLE


@fixture
def attributes():
    return extract_header(TEST_AUDIT_TRAIL, ATTRIBUTE_PRIORITY)


@fixture
def security_master():
    sm_header, sm_table = generate_security_master(TEST_AUDIT_TRAIL, ATTRIBUTE_PRIORITY)

    return sm_header, sm_table


def test_parse_data_type():
    assert parse_data_type([1, 2]) == pa.int64()
    assert parse_data_type([1.0]) == pa.float64()
    assert parse_data_type(["1", "test"]) == pa.string()
    assert parse_data_type([date(2024, 1, 1), date(2022, 1, 1)]) == pa.date32()

    with raises(Exception) as e:
        parse_data_type([1, "1"])

    assert (
        str(e.value)
        == "Could not parse column for Arrow conversion: more than 1 data type present in column"
    )

    with raises(Exception) as e:
        parse_data_type([{}])

    assert (
        str(e.value)
        == "Could not parse column for Arrow conversion: data type not found"
    )

    with raises(Exception) as e:
        parse_data_type([])

    assert (
        str(e.value)
        == "Could not parse column for Arrow conversion: no elements provided"
    )


def test_to_arrow_from_arrow(security_master):
    sm_header, sm_table = security_master

    arrow_table, _ = to_arrow(sm_header, sm_table)
    converted_header, converted_table = from_arrow(arrow_table)

    assert converted_header == sm_header
    assert converted_table == sm_table


def test_to_pandas_from_pandas(security_master):
    sm_header, sm_table = security_master

    df, schema = to_pandas(sm_header, sm_table)
    converted_header, converted_table = from_pandas(df, schema)

    assert converted_header == sm_header
    assert converted_table == sm_table


def test_write_parquet_read_parquet(security_master):
    sm_header, sm_table = security_master

    output_stream, schema = write_parquet(pa.BufferOutputStream(), sm_header, sm_table)
    converted_header, converted_table = read_parquet(
        pa.BufferReader(output_stream.getvalue()), schema
    )

    assert converted_header == sm_header
    assert converted_table == sm_table


def test_write_csv_read_csv(security_master):
    sm_header, sm_table = security_master

    output_stream, convert_options = write_csv(
        pa.BufferOutputStream(), sm_header, sm_table
    )
    converted_header, converted_table = read_csv(
        pa.BufferReader(output_stream.getvalue()), convert_options
    )

    assert converted_header == sm_header
    assert converted_table == sm_table


def test_write_sql_read_sql(security_master):
    sm_header, sm_table = security_master

    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()

    metadata, schema = write_sql(
        engine, metadata, "security_master", sm_header, sm_table
    )
    converted_header, converted_table = read_sql(
        engine, metadata, "security_master", schema
    )

    assert converted_header == sm_header
    assert converted_table == sm_table
