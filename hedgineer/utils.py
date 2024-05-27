from datetime import datetime

parse_date = lambda x: datetime.strptime(x, "%m/%d/%y")
format_date = lambda x: datetime.strftime(x, "%m/%d/%y")
