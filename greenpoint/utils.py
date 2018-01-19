import itertools
import weakref

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


POOLS = weakref.WeakKeyDictionary()


async def get_db(loop):
    global POOLS
    if loop not in POOLS:
        dburl = get_config().get('database')
        if not dburl:
            raise RuntimeError("No `database` in configuration file")
        POOLS[loop] = asyncpg.pool.create_pool(dburl, max_size=50, loop=loop)
        await POOLS[loop]
    return POOLS[loop]


LOCAL_TIMEZONE = tz.gettz()


def parse_date(s):
    """Parse date from string.

    If no timezone is specified, default is assumed to be local time zone.

    :param s: The date to parse.
    :type s: str
    """
    return iso8601.parse_date(s, LOCAL_TIMEZONE).date()
