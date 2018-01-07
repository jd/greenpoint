import bisect
import csv
import datetime
import itertools
import os.path
import re

import attr

import enum

import cachetools.func

import daiquiri

from lxml import etree

import requests

from greenpoint import storage


LOG = daiquiri.getLogger(__name__)

ONE_DAY = datetime.timedelta(days=1)


class QuoteList(dict):
    def __init__(self, quotes):
        super(QuoteList, self).__init__({q.date: q for q in quotes})

    def __getitem__(self, key):
        if isinstance(key, int):
            keys = sorted(self.keys())
            if keys:
                return self[keys[key]]
            raise KeyError(key)
        elif isinstance(key, slice):
            keys = sorted(self.keys())
            if not keys:
                raise KeyError(key)
            if isinstance(key.start, datetime.date):
                key = slice(bisect.bisect_left(keys, key.start),
                            key.stop,
                            key.step)
            if isinstance(key.stop, datetime.date):
                key = slice(key.start,
                            bisect.bisect_right(keys, key.stop),
                            key.step)
            return [self[v] for v in keys[key]]
        return super(QuoteList, self).__getitem__(key)


@attr.s(slots=True, frozen=True)
class Quote(object):
    date = attr.ib(validator=attr.validators.instance_of(datetime.date))
    open = attr.ib(validator=attr.validators.optional(  # noqa
        attr.validators.instance_of(float)))
    close = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(float)))
    high = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(float)))
    low = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(float)))
    volume = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(int)))


class InstrumentType(enum.Enum):
    ETF = "etf"
    STOCK = "stock"
    FUND = "fund"

    @classmethod
    def strtoenum(cls, value):
        if isinstance(value, cls):
            return value
        return cls[value.upper()]


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

    # Source: https://www.google.com/googlefinance/disclaimer/
    GOOGLE_MIC_MAP = {
        "XPAR": "EPA",
        "XBRU": "EBR",
        "XAMS": "AMS",
        "XLON": "LON",
        "XTSE": "CVE",
        "XNAS": "NASDAQ",
        "XNYS": "NYSE",
        "ARCX": "NYSEARCA",
        "XASE": "NYSEAMERICAN",
    }

    @property
    def google_code(self):
        try:
            return self.GOOGLE_MIC_MAP[self.mic]
        except IndexError:
            return None

    YAHOO_MIC_MAP = {
        "XPAR": ".PA",
        "XBRU": ".BR",
        "XAMS": ".AS",
        "XLON": ".L",
        "XNYS": "",
    }

    @property
    def yahoo_code(self):
        try:
            return self.YAHOO_MIC_MAP[self.mic]
        except IndexError:
            return None


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


def _get_exchange_by_mic_if_necessary(value):
    if isinstance(value, Exchange):
        return value
    return get_exchange_by_mic(value)


@attr.s(hash=True)
class Instrument(object):
    isin = attr.ib(validator=attr.validators.instance_of(str),
                   converter=str.upper)
    type = attr.ib(validator=attr.validators.instance_of(InstrumentType),  # noqa
                   converter=InstrumentType.strtoenum,
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
    exchange = attr.ib(
        validator=attr.validators.optional(
            attr.validators.instance_of(Exchange)),
        converter=attr.converters.optional(_get_exchange_by_mic_if_necessary),
        cmp=False)
    currency = attr.ib(
        validator=attr.validators.optional(
            attr.validators.instance_of(str)),
        converter=attr.converters.optional(str.upper))
    quotes = attr.ib(init=False, cmp=False, repr=False,
                     default=attr.Factory(lambda: QuoteList([])),
                     validator=attr.validators.optional(
                         attr.validators.instance_of(QuoteList)))

    @property
    def google_symbol(self):
        if self.exchange is None:
            return
        google_code = self.exchange.google_code
        if google_code is None:
            return
        return google_code + ":" + self.symbol

    @property
    def yahoo_symbol(self):
        if self.exchange is None:
            if self.type == InstrumentType.FUND:
                # NOTE This is probably wrong, but currently we only support
                # fund that are traded in France, so that works for Yahoo.
                return self.isin + ".PA"
            else:
                return
        yahoo_code = self.exchange.yahoo_code
        if yahoo_code is None:
            return
        return self.symbol + yahoo_code

    @staticmethod
    def get_path(isin):
        return os.path.join("instruments", isin.upper())

    @property
    def path(self):
        return self.get_path(self.isin)

    def save(self):
        return storage.save(self.path, self)

    @classmethod
    def load(cls, **kwargs):
        try:
            return storage.load(cls.get_path(kwargs['isin']))
        except FileNotFoundError:
            i = cls(**kwargs)
            i.save()
            return i

    def fetch_quotes_from_boursorama(self, start=None, stop=None):
        r = requests.get("http://www.boursorama.com/recherche/index.phtml?q=" +
                         self.isin)
        try:
            symbol = r.url.split("symbole=")[1]
        except IndexError:
            return

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
            d = datetime.datetime.strptime(
                point['d'][:-6], "%d/%m/%Y").date()
            if start is not None and d < start:
                continue
            if stop is not None and d > stop:
                continue
            yield Quote(
                date=d,
                open=float(point['o']),
                close=float(point['c']),
                high=float(point['h']),
                low=float(point['l']),
                volume=point['v']
            )

    def fetch_quotes_from_lesechos(self, start=None, stop=None):
        if self.exchange:
            exchange = self.exchange.operating_mic
        elif self.type == InstrumentType.FUND:
            exchange = "WMORN"  # Morningstar fund
        else:
            return

        if start is None:
            start = datetime.date(2000, 1, 1)
        if stop is None:
            stop = datetime.datetime.now().date()

        start = start.strftime("%Y%m%d")
        stop = stop.strftime("%Y%m%d")

        r = requests.get("https://lesechos-bourse-fo-cdn.wlb.aw.atos.net" +
                         "/FDS/history.xml?entity=echos&view=ALL" +
                         "&code=" + self.isin +
                         "&codification=ISIN&adjusted=true&base100=false" +
                         "&exchange=" + exchange +
                         "&sessWithNoQuot=false" +
                         "&beginDate=" + start +
                         "&endDate=" + stop +
                         "&computeVar=true")
        xml = etree.fromstring(r.content)
        for history in xml.xpath("//historyResponse/history/historyDt"):
            kwargs = {
                "date": datetime.datetime.strptime(
                    history.get("dt"), "%Y%m%d").date(),
            }
            for (k, kwarg) in (("openPx", "open"),
                               ("closePx", "close"),
                               ("highPx", "high"),
                               ("lowPx", "low"),
                               ("qty", "volume")):
                v = history.get(k)
                if v is not None:
                    v = float(v)
                    if kwarg == "volume":
                        v = int(v)
                kwargs[kwarg] = v

            yield Quote(**kwargs)

    # <td class="lm">Apr 21, 2017
    # <td class="rgt">58.40
    # <td class="rgt">59.90
    # <td class="rgt">58.40
    # <td class="rgt">59.90
    # <td class="rgt rm">2,918

    _GOOGLE_FINANCE_RE = re.compile("<td class=\"lm\">(.+ \d+, \d+)\n"
                                    "<td class=\"rgt\">(.+)\n"
                                    "<td class=\"rgt\">(.+)\n"
                                    "<td class=\"rgt\">(.+)\n"
                                    "<td class=\"rgt\">(.+)\n"
                                    "<td class=\"rgt rm\">(.+)\n")

    def fetch_quotes_from_google(self, start=None, stop=None):
        if self.google_symbol is None:
            LOG.warning("No Google code for %r, cannot fetch quotes", self)
            return

        for index in itertools.count(0, 200):
            r = requests.get(
                "https://finance.google.com/finance/historical"
                "?q=%s&num=200&start=%d"
                % (self.google_symbol, index))

            results = list(self._GOOGLE_FINANCE_RE.finditer(r.text))
            if not len(results):
                return

            for found in results:
                date = datetime.datetime.strptime(
                    found.group(1), "%b %d, %Y").date()
                if start is not None and date < start:
                    # Results are ordered descending
                    # As soon as a date is before the start, we can stop
                    return
                if stop is not None and date > stop:
                    continue
                values = []
                for idx in range(2, 7):
                    v = found.group(idx)
                    if v == "-":
                        break
                    v = float(v.replace(",", ""))
                    values.append(v)
                else:
                    yield Quote(
                        date=date,
                        open=values[0],
                        high=values[1],
                        low=values[2],
                        close=values[3],
                        volume=int(values[4]),
                    )

    QUOTES_PROVIDERS = {
        "boursorama": fetch_quotes_from_boursorama,
        "lesechos": fetch_quotes_from_lesechos,
        "google": fetch_quotes_from_google,
    }

    def refresh_quotes(self, start=None, stop=None):
        """Get quotes from all available providers and merge them.

        :param start: Timestamp to start at (included)
        :param stop: Timestamp to stop at (included)
        """
        quotes = []
        for func in self.QUOTES_PROVIDERS.values():
            quotes.extend(func(self, start, stop))
        self.quotes = QuoteList(quotes)
        self.save()

    def fetch_live_quote_from_yahoo(self):
        if self.yahoo_symbol is None:
            LOG.warning("No Yahoo code for %r, cannot fetch quotes", self)
            return

        r = requests.get("https://query1.finance.yahoo.com/v7/finance/quote?"
                         "lang=en-US&region=US&corsDomain=finance.yahoo.com"
                         "&fields=currency,"
                         "regularMarketDayHigh,"
                         "regularMarketDayLow,"
                         "regularMarketOpen,"
                         "regularMarketPrice,"
                         "regularMarketTime,"
                         "regularMarketVolume"
                         "&symbols=" + self.yahoo_symbol)
        result = r.json()['quoteResponse']['result']
        if not len(result):
            return
        result = result[0]
        currency = result['currency'].upper()
        if currency != self.currency:
            raise ValueError(
                "Quote returned by Yahoo is in "
                "currency %s but instrument uses %s"
                % (currency, self.currency)
            )
        # Safe guard
        if 'regularMarketOpen' not in result:
            return
        return Quote(
            date=datetime.datetime.utcfromtimestamp(
                result['regularMarketTime']).date(),
            open=result['regularMarketOpen'],
            close=result['regularMarketPrice'],
            low=result['regularMarketDayLow'],
            high=result['regularMarketDayHigh'],
            volume=result['regularMarketVolume'],
        )

    @property
    @cachetools.func.ttl_cache(maxsize=8192, ttl=60)
    def quote(self):
        quote = self.fetch_live_quote_from_yahoo()
        if quote is None:
            if self.quotes:
                return self.quotes[sorted(self.quotes.keys())[-1]]
        return quote
