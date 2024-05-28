from datetime import datetime
from typing import Any

parse_date = lambda x: datetime.strptime(x, "%m/%d/%y").date()
format_date = lambda x: datetime.strftime(x, "%m/%d/%y")


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
