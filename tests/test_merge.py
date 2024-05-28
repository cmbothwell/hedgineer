from datetime import date

from pytest import fixture

from hedgineer.collect import extract_attributes
from hedgineer.globals import ATTRIBUTE_PRIORITY, AUDIT_TRAIL
from hedgineer.merge import cascade_new_values, get_value_diffs, merge_flat_fact


@fixture
def attributes():
    return extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)


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
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            400,
            "healthcare",
            "biotechnology",
        ),
    ]


def test_value_diffs(attributes):
    attributes, attribute_index = attributes
    current_row = (
        1,
        date(2024, 1, 1),
        date(2024, 1, 31),
        "equity",
        "TIKR",
        "Ticker Clocks",
        None,
        "consumer_goods",
        "luxury_watches",
    )

    kv_pairs = [("gics_sector", "a"), ("gics_industry", "b")]

    assert get_value_diffs(current_row, kv_pairs, attribute_index) == [
        (attribute_index["gics_sector"] + 3, "consumer_goods", "a"),
        (attribute_index["gics_industry"] + 3, "luxury_watches", "b"),
    ]


def test_cascade_new_values():
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
            None,
            "consumer_goods",
            "luxury_watches",
        ),
    ]

    table = original_table.copy()
    value_diffs = [(4, "TIKR", "TICK")]
    cascade_new_values(table, 1, 1, value_diffs)
    assert table == [
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
            None,
            "consumer_goods",
            "luxury_watches",
        ),
    ]

    table = original_table.copy()
    value_diffs = [(4, "TIKR", "TICK"), (6, None, "A"), (7, None, "B")]
    cascade_new_values(table, 1, 1, value_diffs)
    assert table == [
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
            "A",
            "consumer_goods",
            "luxury_watches",
        ),
    ]


def test_merge_flat_fact_new_security(attributes, base_table):
    attributes, attribute_index = attributes
    assert merge_flat_fact(
        base_table.copy(),
        (
            2,
            date(2024, 3, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
        attributes,
        attribute_index,
    ) == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            400,
            "healthcare",
            "biotechnology",
        ),
        (
            2,
            date(2024, 3, 1),
            None,
            None,
            None,
            None,
            100,
            "new_a",
            "new_b",
        ),
    ]

    assert merge_flat_fact(
        [
            *base_table,
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
        (
            2,
            date(2024, 3, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
        attributes,
        attribute_index,
    ) == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            400,
            "healthcare",
            "biotechnology",
        ),
        (
            2,
            date(2024, 3, 1),
            None,
            None,
            None,
            None,
            100,
            "new_a",
            "new_b",
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


def test_merge_flat_fact_insert_before(attributes, base_table):
    attributes, attribute_index = attributes
    assert merge_flat_fact(
        base_table,
        (
            1,
            date(2023, 1, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
        attributes,
        attribute_index,
    ) == [
        (
            1,
            date(2023, 1, 1),
            date(2024, 1, 1),
            None,
            None,
            None,
            100,
            "new_a",
            "new_b",
        ),
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            100,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            100,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            400,
            "healthcare",
            "biotechnology",
        ),
    ]


def test_merge_flat_fact_insert_after(attributes, base_table):
    attributes, attribute_index = attributes
    assert merge_flat_fact(
        base_table,
        (
            1,
            date(2024, 6, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
        attributes,
        attribute_index,
    ) == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 5, 23),
            date(2024, 6, 1),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            400,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 6, 1),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            100,
            "new_a",
            "new_b",
        ),
    ]


def test_merge_flat_fact_merge(attributes, base_table):
    attributes, attribute_index = attributes
    assert merge_flat_fact(
        base_table,
        (
            1,
            date(2024, 3, 1),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
        attributes,
        attribute_index,
    ) == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 1),
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 3, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            100,
            "new_a",
            "new_b",
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            100,
            "new_a",
            "new_b",
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            400,
            "new_a",
            "new_b",
        ),
    ]


def test_merge_flat_fact_split(attributes, base_table):
    attributes, attribute_index = attributes
    assert merge_flat_fact(
        base_table,
        (
            1,
            date(2024, 3, 22),
            [("gics_sector", "new_a"), ("gics_industry", "new_b"), ("market_cap", 100)],
        ),
        attributes,
        attribute_index,
    ) == [
        (
            1,
            date(2024, 1, 1),
            date(2024, 3, 22),
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            date(2024, 3, 22),
            date(2024, 5, 23),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            100,
            "new_a",
            "new_b",
        ),
        (
            1,
            date(2024, 5, 23),
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            400,
            "new_a",
            "new_b",
        ),
    ]
