from datetime import date

from pytest import fixture

from hedgineer.collect import (
    accumulate_fact,
    bucket_fact,
    bucket_facts,
    deeply_spread,
    diff_row,
    extract_attributes,
    flatten_and_sort_facts,
    generate_empty_row,
    generate_security_master,
    generate_security_master_from_facts,
    join_positions,
)
from hedgineer.globals import ATTRIBUTE_PRIORITY, AUDIT_TRAIL, POSITIONS_TABLE
from hedgineer.utils import parse_date


@fixture
def audit_trail():
    return AUDIT_TRAIL


@fixture
def attribute_priority():
    return ATTRIBUTE_PRIORITY


@fixture
def positions_table():
    return POSITIONS_TABLE


@fixture
def nested_dict():
    return {
        "a": {
            "1": {"x": 0, "y": [1, 2]},
            "2": {"x": 0, "y": [1, 2]},
        },
        "b": {
            "1": {"x": 0, "y": [1, 2]},
            "2": {"x": 0, "y": [1, 2]},
        },
    }


@fixture
def attributes():
    attributes, _ = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
    return attributes


@fixture
def bucketed_facts(audit_trail):
    return bucket_facts(audit_trail)


@fixture
def sorted_flat_facts(bucketed_facts):
    return flatten_and_sort_facts(bucketed_facts)


@fixture
def security_master(sorted_flat_facts):
    attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
    return generate_security_master_from_facts(
        sorted_flat_facts, attributes, attribute_index
    )


def test_deeply_spread(nested_dict):
    assert deeply_spread(nested_dict) == [
        ("a", "1", "x", 0),
        ("a", "1", "y", [1, 2]),
        ("a", "2", "x", 0),
        ("a", "2", "y", [1, 2]),
        ("b", "1", "x", 0),
        ("b", "1", "y", [1, 2]),
        ("b", "2", "x", 0),
        ("b", "2", "y", [1, 2]),
    ]
    assert deeply_spread(nested_dict["a"]) == [
        ("1", "x", 0),
        ("1", "y", [1, 2]),
        ("2", "x", 0),
        ("2", "y", [1, 2]),
    ]
    assert deeply_spread(nested_dict["a"]["1"]) == [("x", 0), ("y", [1, 2])]
    assert deeply_spread({}) == []


def test_bucket_fact():
    bucket = {}

    bucket_fact(bucket, (1, "gics_sector", "technology", parse_date("01/01/24")))
    assert bucket == {1: {date(2024, 1, 1): [("gics_sector", "technology")]}}

    bucket_fact(bucket, (1, "name", "LENZ", parse_date("01/01/24")))
    assert bucket == {
        1: {date(2024, 1, 1): [("gics_sector", "technology"), ("name", "LENZ")]}
    }

    bucket_fact(bucket, (1, "gics_sector", "healthcare", parse_date("03/01/24")))
    bucket_fact(bucket, (1, "name", "New Name", parse_date("03/01/24")))
    assert bucket == {
        1: {
            date(2024, 1, 1): [("gics_sector", "technology"), ("name", "LENZ")],
            date(2024, 3, 1): [("gics_sector", "healthcare"), ("name", "New Name")],
        }
    }

    bucket_fact(bucket, (2, "name", "ACME Corp.", parse_date("01/01/22")))
    assert bucket == {
        1: {
            date(2024, 1, 1): [("gics_sector", "technology"), ("name", "LENZ")],
            date(2024, 3, 1): [("gics_sector", "healthcare"), ("name", "New Name")],
        },
        2: {date(2022, 1, 1): [("name", "ACME Corp.")]},
    }


def test_flatten_and_sort_facts(bucketed_facts):
    assert flatten_and_sort_facts(bucketed_facts) == [
        (
            1,
            date(2024, 1, 1),
            [
                ("gics_sector", "healthcare"),
                ("ticker", "GRPH"),
                ("gics_industry", "biotechnology"),
                ("asset_class", "equity"),
                ("name", "Graphite bio"),
            ],
        ),
        (
            1,
            date(2024, 3, 22),
            [("ticker", "LENZ"), ("name", "Lenz Therapeutics, Inc")],
        ),
        (1, date(2024, 5, 23), [("market_cap", 400)]),
        (
            2,
            date(2023, 1, 1),
            [("ticker", "V"), ("gics_sector", "technology")],
        ),
        (2, date(2023, 3, 17), [("gics_sector", "financials")]),
        (2, date(2024, 5, 23), [("market_cap", 549000)]),
    ]


def test_generate_empty_row():
    assert generate_empty_row(4) == (None, None, None, None)
    assert generate_empty_row(5) == (None, None, None, None, None)
    assert generate_empty_row(0) == ()


def test_diff_row(audit_trail, attribute_priority):
    _, attribute_index = extract_attributes(audit_trail, attribute_priority)
    prior_row = (
        1,
        date(24, 1, 1),
        None,
        "equity",
        "GRPH",
        "Graphite bio",
        None,
        "healthcare",
        "biotechnology",
    )
    new_row = diff_row(
        prior_row,
        1,
        date(24, 3, 22),
        attribute_index,
        [("ticker", "LENZ"), ("name", "Lenz Therapeutics, Inc")],
    )

    assert new_row == (
        1,
        date(24, 3, 22),
        None,
        "equity",
        "LENZ",
        "Lenz Therapeutics, Inc",
        None,
        "healthcare",
        "biotechnology",
    )


def test_accumulate_fact(audit_trail, attribute_priority):
    attributes, attribute_index = extract_attributes(audit_trail, attribute_priority)
    sm_table = []

    flat_fact = (
        1,
        date(2024, 1, 1),
        [
            ("gics_sector", "healthcare"),
            ("ticker", "GRPH"),
            ("gics_industry", "biotechnology"),
            ("asset_class", "equity"),
            ("name", "Graphite bio"),
        ],
    )
    accumulate_fact((sm_table, attributes, attribute_index), flat_fact)

    assert sm_table == [
        (
            1,
            date(2024, 1, 1),
            None,
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        )
    ]

    flat_fact = (
        1,
        date(2024, 3, 22),
        [
            ("ticker", "LENZ"),
            ("name", "Lenz Therapeutics, Inc"),
        ],
    )
    accumulate_fact((sm_table, attributes, attribute_index), flat_fact)

    assert sm_table == [
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
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            None,
            "healthcare",
            "biotechnology",
        ),
    ]


def test_generate_security_master_from_facts(
    audit_trail, attribute_priority, sorted_flat_facts
):
    attributes, attribute_index = extract_attributes(audit_trail, attribute_priority)
    security_master = generate_security_master_from_facts(
        sorted_flat_facts, attributes, attribute_index
    )

    assert security_master == [
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
            date(2023, 1, 1),
            date(2023, 3, 17),
            None,
            "V",
            None,
            None,
            "technology",
            None,
        ),
        (
            2,
            date(2023, 3, 17),
            date(2024, 5, 23),
            None,
            "V",
            None,
            None,
            "financials",
            None,
        ),
        (
            2,
            date(2024, 5, 23),
            None,
            None,
            "V",
            None,
            549000,
            "financials",
            None,
        ),
    ]


def test_generate_security_master(audit_trail, attribute_priority):
    _, security_master = generate_security_master(audit_trail, attribute_priority)
    assert security_master == [
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
            date(2023, 1, 1),
            date(2023, 3, 17),
            None,
            "V",
            None,
            None,
            "technology",
            None,
        ),
        (
            2,
            date(2023, 3, 17),
            date(2024, 5, 23),
            None,
            "V",
            None,
            None,
            "financials",
            None,
        ),
        (
            2,
            date(2024, 5, 23),
            None,
            None,
            "V",
            None,
            549000,
            "financials",
            None,
        ),
    ]


def test_join_positions(security_master, positions_table, attributes):
    _, joined_positions = join_positions(attributes, security_master, positions_table)
    assert joined_positions == [
        (
            1,
            100,
            date(2024, 2, 1),
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            105,
            date(2024, 2, 1),
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            2,
            150,
            date(2024, 2, 1),
            None,
            "V",
            None,
            None,
            "financials",
            None,
        ),
        (
            1,
            120,
            date(2024, 3, 1),
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            2,
            140,
            date(2024, 3, 1),
            None,
            "V",
            None,
            None,
            "financials",
            None,
        ),
    ]


def test_extract_attributes():
    attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)

    assert set(attributes) == set(list(zip(*AUDIT_TRAIL))[1])
    for k, v in attribute_index.items():
        assert v == attributes.index(k)
        if k in ATTRIBUTE_PRIORITY:
            assert v == ATTRIBUTE_PRIORITY[k]


def test_bucket_facts(audit_trail):
    assert bucket_facts(audit_trail) == {
        1: {
            date(2024, 3, 22): [
                ("ticker", "LENZ"),
                ("name", "Lenz Therapeutics, Inc"),
            ],
            date(2024, 1, 1): [
                ("gics_sector", "healthcare"),
                ("ticker", "GRPH"),
                ("gics_industry", "biotechnology"),
                ("asset_class", "equity"),
                ("name", "Graphite bio"),
            ],
            date(2024, 5, 23): [("market_cap", 400)],
        },
        2: {
            date(2024, 5, 23): [("market_cap", 549000)],
            date(2023, 1, 1): [
                ("ticker", "V"),
                ("gics_sector", "technology"),
            ],
            date(2023, 3, 17): [("gics_sector", "financials")],
        },
    }
