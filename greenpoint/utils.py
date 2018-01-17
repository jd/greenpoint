import itertools

import asyncpg.pool

from dateutil import tz

import iso8601

import yaml


def grouper(iterable, n):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def get_config():
    with open("config.yaml") as f:
        return yaml.load(f.read())


POOL = None


async def get_db():
    global POOL
    if POOL is None:
        dburl = get_config().get('database')
        if not dburl:
            raise RuntimeError("No `database` in configuration file")
        POOL = asyncpg.pool.create_pool(dburl, max_size=50)
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
