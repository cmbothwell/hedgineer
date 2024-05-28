from datetime import datetime, timedelta
from random import choice, randint
from typing import Any

from hedgineer.globals import MOCK_ATTRIBUTES


def parse_date(x):
    return datetime.strptime(x, "%m/%d/%y").date()


def format_date(x):
    return datetime.strftime(x, "%m/%d/%y")


def replace_at_index(t: tuple, i: int, v):
    return (*t[:i], v, *t[i + 1 :])


def generate_none_tuple(length):
    return tuple(map(lambda _: None, range(length)))


def deeply_spread(dd: dict[Any, Any]):
    result = []

    for k, v in dd.items():
        if isinstance(v, dict):
            result.extend(map(lambda k_: (k, *k_), deeply_spread(v)))
        else:
            result.append((k, v))

    return result


def random_attribute_pair():
    key = choice(list(MOCK_ATTRIBUTES.keys()))
    val = MOCK_ATTRIBUTES[key]()

    return key, val


def random_day(start_date, end_date):
    delta = (end_date - start_date).days
    return format_date(start_date + timedelta(days=randint(0, delta)))
