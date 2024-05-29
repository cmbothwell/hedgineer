from datetime import date

from pytest import fixture

from hedgineer.collect import (
    accumulate_fact,
    bucket_fact,
    bucket_facts,
    deeply_spread,
    diff_row,
    extract_header,
    flatten_and_sort_facts,
    generate_data_from_facts,
    generate_security_master,
    join_positions,
)
from hedgineer.globals import ATTRIBUTE_PRIORITY, POSITIONS_TABLE, TEST_AUDIT_TRAIL
from hedgineer.utils import generate_none_tuple, parse_date


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
def security_master(audit_trail, attribute_priority):
    return generate_security_master(audit_trail, attribute_priority)


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
    assert generate_none_tuple(4) == (None, None, None, None)
    assert generate_none_tuple(5) == (None, None, None, None, None)
    assert generate_none_tuple(0) == ()


def test_diff_row(audit_trail, attribute_priority):
    _, col_index = extract_header(audit_trail, attribute_priority)
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
        col_index,
        (1, date(24, 3, 22), [("ticker", "LENZ"), ("name", "Lenz Therapeutics, Inc")]),
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
    _, col_index = extract_header(audit_trail, attribute_priority)
    data = []
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

    accumulate_fact((data, col_index), flat_fact)
    assert data == [
        (
            1,
            date(2024, 1, 1),
            None,
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
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

    accumulate_fact((data, col_index), flat_fact)
    assert data == [
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
            None,
            "equity",
            "LENZ",
            "Lenz Therapeutics, Inc",
            "biotechnology",
            "healthcare",
            None,
        ),
    ]


def test_generate_data_from_facts(audit_trail, attribute_priority, sorted_flat_facts):
    _, col_index = extract_header(audit_trail, attribute_priority)
    data = generate_data_from_facts(sorted_flat_facts, col_index)

    assert data == [
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
            None,
            "financials",
            549000,
        ),
    ]


def test_generate_security_master(audit_trail, attribute_priority):
    sm = generate_security_master(audit_trail, attribute_priority)
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
            None,
            "financials",
            549000,
        ),
    ]


def test_join_positions(security_master, positions_table):
    jp = join_positions(security_master, positions_table)
    assert jp.data == [
        (
            1,
            100,
            date(2024, 1, 1),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            1,
            105,
            date(2024, 1, 2),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            2,
            150,
            date(2024, 1, 2),
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
            date(2024, 1, 3),
            "equity",
            "GRPH",
            "Graphite bio",
            "biotechnology",
            "healthcare",
            None,
        ),
        (
            2,
            140,
            date(2024, 1, 3),
            None,
            "V",
            None,
            None,
            "financials",
            None,
        ),
    ]


def test_extract_header():
    header, col_index = extract_header(TEST_AUDIT_TRAIL, ATTRIBUTE_PRIORITY)

    assert set(header) == set(
        (
            "security_id",
            "effective_start_date",
            "effective_end_date",
            *list(zip(*TEST_AUDIT_TRAIL))[1],
        )
    )
    for k, v in col_index.items():
        assert v == header.index(k)
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
