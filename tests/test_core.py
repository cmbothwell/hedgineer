from datetime import datetime

from pytest import fixture

from hedgineer.core import (
    bucket_fact,
    bucket_facts,
    deeply_spread,
    extract_attributes,
    flatten_and_sort_facts,
    generate_security_master,
    join_positions,
)
from hedgineer.globals import ATTRIBUTE_PRIORITY, AUDIT_TRAIL, POSITIONS_TABLE
from hedgineer.utils import parse_date


@fixture
def audit_trail():
    return AUDIT_TRAIL


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
def bucketed_facts(audit_trail):
    return bucket_facts(audit_trail)


@fixture
def sorted_flat_facts(bucketed_facts):
    return flatten_and_sort_facts(bucketed_facts)


@fixture
def security_master(sorted_flat_facts):
    attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
    return generate_security_master(sorted_flat_facts, attributes, attribute_index)


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
    assert bucket == {1: {datetime(2024, 1, 1): [("gics_sector", "technology")]}}

    bucket_fact(bucket, (1, "name", "LENZ", parse_date("01/01/24")))
    assert bucket == {
        1: {datetime(2024, 1, 1): [("gics_sector", "technology"), ("name", "LENZ")]}
    }

    bucket_fact(bucket, (1, "gics_sector", "healthcare", parse_date("03/01/24")))
    bucket_fact(bucket, (1, "name", "New Name", parse_date("03/01/24")))
    assert bucket == {
        1: {
            datetime(2024, 1, 1): [("gics_sector", "technology"), ("name", "LENZ")],
            datetime(2024, 3, 1): [("gics_sector", "healthcare"), ("name", "New Name")],
        }
    }

    bucket_fact(bucket, (2, "name", "ACME Corp.", parse_date("01/01/22")))
    assert bucket == {
        1: {
            datetime(2024, 1, 1): [("gics_sector", "technology"), ("name", "LENZ")],
            datetime(2024, 3, 1): [("gics_sector", "healthcare"), ("name", "New Name")],
        },
        2: {datetime(2022, 1, 1): [("name", "ACME Corp.")]},
    }


def test_flatten_and_sort_facts(bucketed_facts):
    assert flatten_and_sort_facts(bucketed_facts) == [
        (
            1,
            datetime(2024, 1, 1, 0, 0),
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
            datetime(2024, 3, 22, 0, 0),
            [("ticker", "LENZ"), ("name", "Lenz Therapeutics, Inc")],
        ),
        (1, datetime(2024, 5, 23, 0, 0), [("market_cap", 400)]),
        (
            2,
            datetime(2023, 1, 1, 0, 0),
            [("ticker", "V"), ("gics_sector", "technology")],
        ),
        (2, datetime(2023, 3, 17, 0, 0), [("gics_sector", "financials")]),
        (2, datetime(2024, 5, 23, 0, 0), [("market_cap", 549000)]),
    ]


def test_generate_security_master(sorted_flat_facts):
    attributes, attribute_index = extract_attributes(AUDIT_TRAIL, ATTRIBUTE_PRIORITY)
    assert generate_security_master(sorted_flat_facts, attributes, attribute_index) == [
        (
            1,
            datetime(2024, 1, 1, 0, 0),
            datetime(2024, 3, 22, 0, 0),
            "equity",
            "GRPH",
            "Graphite bio",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            datetime(2024, 3, 22, 0, 0),
            datetime(2024, 5, 23, 0, 0),
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            None,
            "healthcare",
            "biotechnology",
        ),
        (
            1,
            datetime(2024, 5, 23, 0, 0),
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
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 3, 17, 0, 0),
            None,
            "V",
            None,
            None,
            "technology",
            None,
        ),
        (
            2,
            datetime(2023, 3, 17, 0, 0),
            datetime(2024, 5, 23, 0, 0),
            None,
            "V",
            None,
            None,
            "financials",
            None,
        ),
        (
            2,
            datetime(2024, 5, 23, 0, 0),
            None,
            None,
            "V",
            None,
            549000,
            "financials",
            None,
        ),
    ]


def test_join_positions(security_master, positions_table):
    assert join_positions(security_master, positions_table) == [
        (
            1,
            100,
            datetime(2024, 2, 1, 0, 0),
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
            datetime(2024, 2, 1, 0, 0),
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
            datetime(2024, 2, 1, 0, 0),
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
            datetime(2024, 3, 1, 0, 0),
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
            datetime(2024, 3, 1, 0, 0),
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
            datetime(2024, 3, 22, 0, 0): [
                ("ticker", "LENZ"),
                ("name", "Lenz Therapeutics, Inc"),
            ],
            datetime(2024, 1, 1, 0, 0): [
                ("gics_sector", "healthcare"),
                ("ticker", "GRPH"),
                ("gics_industry", "biotechnology"),
                ("asset_class", "equity"),
                ("name", "Graphite bio"),
            ],
            datetime(2024, 5, 23, 0, 0): [("market_cap", 400)],
        },
        2: {
            datetime(2024, 5, 23, 0, 0): [("market_cap", 549000)],
            datetime(2023, 1, 1, 0, 0): [
                ("ticker", "V"),
                ("gics_sector", "technology"),
            ],
            datetime(2023, 3, 17, 0, 0): [("gics_sector", "financials")],
        },
    }
