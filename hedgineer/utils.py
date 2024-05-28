from datetime import datetime

parse_date = lambda x: datetime.strptime(x, "%m/%d/%y").date()
format_date = lambda x: datetime.strftime(x, "%m/%d/%y")


def replace_at_index(t: tuple, i: int, v):
    return (*t[:i], v, *t[i + 1 :])
