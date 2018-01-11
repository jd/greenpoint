import itertools

import asyncpg.pool

from dateutil import tz

import iso8601


def grouper(iterable, n):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


POOL = asyncpg.pool.create_pool("postgresql:///greenpoint", max_size=50)


async def get_db():
    await POOL
    return POOL


LOCAL_TIMEZONE = tz.gettz()


def parse_date(s):
    """Parse date from string.

    If no timezone is specified, default is assumed to be local time zone.

    :param s: The date to parse.
    :type s: str
    """
    return iso8601.parse_date(s, LOCAL_TIMEZONE).date()
