import csv
import datetime
import os.path

import attr

import enum

import leven

import orderedset

import requests


@attr.s(slots=True, frozen=True)
class Quote(object):
    date = attr.ib(validator=attr.validators.instance_of(datetime.date))
    open = attr.ib(validator=attr.validators.instance_of(float))  # noqa
    close = attr.ib(validator=attr.validators.instance_of(float))
    high = attr.ib(validator=attr.validators.instance_of(float))
    low = attr.ib(validator=attr.validators.instance_of(float))
    volume = attr.ib(validator=attr.validators.instance_of(int))


class InstrumentType(enum.Enum):
    ETF = "etf"
    STOCK = "stock"
    FUND = "fund"


@attr.s(frozen=True)
class Exchange(object):
    mic = attr.ib(validator=attr.validators.instance_of(str),
                  converter=str.upper)
    operating_mic = attr.ib(validator=attr.validators.instance_of(str),
                            converter=str.upper,
                            cmp=False)
    name = attr.ib(validator=attr.validators.instance_of(str), cmp=False)
    country = attr.ib(validator=attr.validators.instance_of(str), cmp=False)
    country_code = attr.ib(validator=attr.validators.instance_of(str),
                           cmp=False)
    city = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(str)), cmp=False)
    comments = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(str)), cmp=False)


def list_exchanges():
    with open(
            os.path.join(
                os.path.dirname(__file__), "data/ISO10383_MIC.csv"),
            "r", encoding="latin-1") as f:
        for row in csv.DictReader(f):
            yield Exchange(
                mic=row['MIC'],
                operating_mic=row['OPERATING MIC'],
                name=row['NAME-INSTITUTION DESCRIPTION'],
                country=row['COUNTRY'],
                country_code=row['ISO COUNTRY CODE (ISO 3166)'],
                city=row['CITY'],
                comments=row['COMMENTS'])


Exchanges = list(list_exchanges())
ExchangesMICMap = {e.mic: e for e in Exchanges}


def get_exchange_by_mic(mic):
    return ExchangesMICMap[mic]


def _leven_ex(target, ex_name):
    return leven.levenshtein(
        target,
        " ".join(orderedset.OrderedSet(ex_name.split(" "))))


def get_exchange_by_name(name):
    lower = name.lower()
    return list(sorted(
        Exchanges,
        key=lambda ex: _leven_ex(lower, ex.name.lower())))[0]


@attr.s(frozen=True)
class Instrument(object):
    isin = attr.ib(validator=attr.validators.instance_of(str),
                   converter=str.upper)
    type = attr.ib(validator=attr.validators.instance_of(InstrumentType),  # noqa
                   cmp=False)
    name = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(str)), cmp=False)
    symbol = attr.ib(
        validator=attr.validators.optional(
            attr.validators.instance_of(str)),
        converter=attr.converters.optional(str.upper),
        cmp=False)
    pea = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(bool)), cmp=False)
    pea_pme = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(bool)), cmp=False)
    ttf = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(bool)), cmp=False)
    exchange = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(Exchange)), cmp=False)

    def get_quotes_from_boursorama(self):
        r = requests.get("http://www.boursorama.com/recherche/index.phtml?q=" +
                         self.isin)
        try:
            symbol = r.url.split("symbole=")[1]
        except IndexError:
            raise ValueError("Unable to find quote for %s" % self.isin)

        r = requests.get(
            "http://www.boursorama.com/graphiques/quotes.phtml?s%5B0%5D=" +
            symbol +
            "&c=eJxNUEFuwyAQzFv22BPYbRqWYw9RpapNZTV"
            "SThaxabwqDhHgWlHkvxdsXCKVE7Mz"
            "s8ygUODNY4FALUiPzwjH8luINU_oCWGkNnQgC"
            "TeMxckaodN06kIaiYJli2p-9HleUFwiV"
            "PttugmEwZmdcqr3IBXy9BBHuAPC5OYMge1YPIf"
            "qUICcpjhLqs6O1dD3yl1BHpFnKZ1bal"
            "Sw7v-WDywfEb6o3jp16aipPwcbtK9f7-J6b83"
            "Qa4g9b3Or1cNq6Rv7_C5cXASbmQgUjM5"
            "cXPYtqVLRwRgdXqyx7k2f5so5W4zckr8YdX3Xo0-_w-T0B0RLa1wn"
        )

        for point in r.json()['dataSets'][0]['dataProvider']:
            yield Quote(
                date=datetime.datetime.strptime(
                    point['d'][:-6], "%d/%m/%Y").date(),
                open=float(point['o']),
                close=float(point['c']),
                high=float(point['h']),
                low=float(point['l']),
                volume=point['v']
            )
