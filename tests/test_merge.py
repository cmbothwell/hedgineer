from datetime import date

from pytest import fixture

from hedgineer.collect import extract_header
from hedgineer.globals import ATTRIBUTE_PRIORITY, TEST_AUDIT_TRAIL
from hedgineer.merge import cascade_new_values, get_value_diffs, merge_flat_fact
from hedgineer.types import SecurityMaster


@fixture
def extract_head():
    return extract_header(TEST_AUDIT_TRAIL, ATTRIBUTE_PRIORITY)


@fixture
def base_table():
    return [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            400,
        ),
    ]


def test_value_diffs(extract_head, base_table):
    header, col_index = extract_head
    sm = SecurityMaster.from_tuple((header, base_table, col_index))

    current_row = (
        1,
        date(2024, 1, 1),
        date(2024, 1, 31),
        "equity",
        "TIKR",
        "Ticker Clocks",
        "luxury_watches",
        "consumer_goods",
        None,
    )
    kv_pairs = [("gics_sector", "a"), ("gics_industry", "b")]

    assert get_value_diffs(sm, current_row, kv_pairs) == [
        (sm.col_index["gics_sector"], "consumer_goods", "a"),
        (sm.col_index["gics_industry"], "luxury_watches", "b"),
    ]


def test_cascade_new_values(extract_head):
    header, col_index = extract_head
    original_table = [
        (
            1,
            date(2024, 1, 1),
            date(2024, 2, 1),
            "equity",
            "TIKR",
            "Ticker Clocks",
            None,
            None,
            None,
        ),
        (
            1,
            date(2024, 2, 1),
            date(2024, 3, 1),
            "equity",
            "TIKR",
            "Ticker Clocks",
            None,
            None,
            None,
        ),
        (
            1,
            date(2024, 3, 1),
            date(2024, 4, 1),
            "equity",
            "TIKR",
            "Ticker Clocks",
            "luxury_watches",
            "consumer_goods",
            None,
        ),
    ]

    sm = SecurityMaster.from_tuple((header, original_table.copy(), col_index))
    value_diffs = [(4, "TIKR", "TICK")]
    cascade_new_values(sm, 1, 1, value_diffs)
    assert sm.data == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 2, 1),
            "equity",
            "TIKR",
            "Ticker Clocks",
            None,
            None,
            None,
        ),
        (
            1,
            date(2024, 2, 1),
            date(2024, 3, 1),
            "equity",
            "TICK",
            "Ticker Clocks",
            None,
            None,
            None,
        ),
        (
            1,
            date(2024, 3, 1),
            date(2024, 4, 1),
            "equity",
            "TICK",
            "Ticker Clocks",
            "luxury_watches",
            "consumer_goods",
            None,
        ),
    ]

    sm = SecurityMaster.from_tuple((header, original_table.copy(), col_index))
    value_diffs = [(4, "TIKR", "TICK"), (6, None, "A"), (7, None, "B")]
    cascade_new_values(sm, 1, 1, value_diffs)
    assert sm.data == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 2, 1),
            "equity",
            "TIKR",
            "Ticker Clocks",
            None,
            None,
            None,
        ),
        (
            1,
            date(2024, 2, 1),
            date(2024, 3, 1),
            "equity",
            "TICK",
            "Ticker Clocks",
            "A",
            "B",
            None,
        ),
        (
            1,
            date(2024, 3, 1),
            date(2024, 4, 1),
            "equity",
            "TICK",
            "Ticker Clocks",
            "luxury_watches",
            "consumer_goods",
            None,
        ),
    ]


def test_merge_flat_fact_new_security(extract_head, base_table):
    header, col_index = extract_head
    sm = SecurityMaster.from_tuple((header, base_table.copy(), col_index))
    sm = merge_flat_fact(
        sm,
        (
            2,
            date(2024, 3, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
    )
    assert sm.data == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            400,
        ),
        (
            2,
            date(2024, 3, 1),
            None,
            None,
            None,
            None,
            "new_b",
            "new_a",
            100,
        ),
    ]

    sm = SecurityMaster.from_tuple(
        (
            header,
            [
                *base_table.copy(),
                (
                    3,
                    date(2024, 3, 1),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            ],
            col_index,
        )
    )
    sm = merge_flat_fact(
        sm,
        (
            2,
            date(2024, 3, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
    )
    assert sm.data == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            400,
        ),
        (
            2,
            date(2024, 3, 1),
            None,
            None,
            None,
            None,
            "new_b",
            "new_a",
            100,
        ),
        (
            3,
            date(2024, 3, 1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    ]


def test_merge_flat_fact_insert_before(extract_head, base_table):
    header, col_index = extract_head
    sm = SecurityMaster.from_tuple((header, base_table.copy(), col_index))
    sm = merge_flat_fact(
        sm,
        (
            1,
            date(2023, 1, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
    )

    assert sm.data == [
        (
            1,
            date(2023, 1, 1),
            date(2024, 1, 1),
            None,
            None,
            None,
            "new_b",
            "new_a",
            100,
        ),
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            100,
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            100,
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            400,
        ),
    ]


def test_merge_flat_fact_insert_after(extract_head, base_table):
    header, col_index = extract_head
    sm = SecurityMaster.from_tuple((header, base_table.copy(), col_index))

    sm = merge_flat_fact(
        sm,
        (
            1,
            date(2024, 6, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
    )

    assert sm.data == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 5, 23),
            date(2024, 6, 1),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            400,
        ),
        (
            1,
            date(2024, 6, 1),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "new_b",
            "new_a",
            100,
        ),
    ]


def test_merge_flat_fact_split(extract_head, base_table):
    header, col_index = extract_head
    sm = SecurityMaster.from_tuple((header, base_table.copy(), col_index))
    sm = merge_flat_fact(
        sm,
        (
            1,
            date(2024, 3, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
    )

    assert sm.data == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 1),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 3, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            "new_b",
            "new_a",
            100,
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "new_b",
            "new_a",
            100,
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "new_b",
            "new_a",
            400,
        ),
    ]


def test_merge_flat_fact_merge(extract_head, base_table):
    header, col_index = extract_head
    sm = SecurityMaster.from_tuple((header, base_table.copy(), col_index))
    sm = merge_flat_fact(
        sm,
        (
            1,
            date(2024, 3, 22),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
    )

    assert sm.data == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "new_b",
            "new_a",
            100,
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "new_b",
            "new_a",
            400,
        ),
    ]
