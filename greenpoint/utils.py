from dateutil import tz
import itertools
import iso8601


def grouper(iterable, n):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


LOCAL_TIMEZONE = tz.gettz()


def parse_date(s):
    """Parse date from string.

    If no timezone is specified, default is assumed to be local time zone.

    :param s: The date to parse.
    :type s: str
    """
    return iso8601.parse_date(s, LOCAL_TIMEZONE).date()
