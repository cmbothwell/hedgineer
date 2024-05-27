from datetime import datetime

from pytest import fixture

from hedgineer.core import bucket_fact, bucket_facts, deeply_spread, extract_attributes
from hedgineer.globals import ATTRIBUTE_PRIORITY, AUDIT_TRAIL
from hedgineer.utils import parse_date


@fixture
def audit_trail():
    return AUDIT_TRAIL


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


def test_join_position():
    pass


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
