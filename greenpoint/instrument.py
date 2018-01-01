import csv
import datetime
import os.path
import re

import attr

import enum

import leven

from lxml import etree

import orderedset

import requests

from greenpoint import storage


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
    }

    @property
    def google_code(self):
        try:
            return self.GOOGLE_MIC_MAP[self.mic]
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


def _leven_ex(target, ex_name):
    if target in ex_name:
        return -1
    return leven.levenshtein(
        target,
        " ".join(orderedset.OrderedSet(ex_name.split(" "))))


def get_exchange_by_name(name):
    lower = name.lower()
    return list(sorted(
        Exchanges,
        key=lambda ex: _leven_ex(lower, ex.name.lower())))[0]


@attr.s(hash=True)
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
    _quotes = attr.ib(init=False, default=None)

    def fetch_quotes_from_boursorama(self):
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
            yield Quote(
                date=datetime.datetime.strptime(
                    point['d'][:-6], "%d/%m/%Y").date(),
                open=float(point['o']),
                close=float(point['c']),
                high=float(point['h']),
                low=float(point['l']),
                volume=point['v']
            )

    def fetch_quotes_from_lesechos(self):
        end = datetime.datetime.now().date().strftime("%Y%m%d")
        r = requests.get("https://lesechos-bourse-fo-cdn.wlb.aw.atos.net" +
                         "/FDS/history.xml?entity=echos&view=ALL" +
                         "&code=" + self.isin +
                         "&codification=ISIN&adjusted=true&base100=false" +
                         "&exchange=" + self.exchange.operating_mic +
                         "&sessWithNoQuot=false&beginDate=19000101&endDate=" +
                         end + "&computeVar=true")
        xml = etree.fromstring(r.content)
        for history in xml.xpath("//historyResponse/history/historyDt"):
            yield Quote(
                date=datetime.datetime.strptime(
                    history.get("dt"), "%Y%m%d").date(),
                open=float(history.get("openPx")),
                close=float(history.get("closePx")),
                high=float(history.get("highPx")),
                low=float(history.get("lowPx")),
                volume=int(float(history.get("qty"))),
            )

    _GOOGLE_FINANCE_RE = re.compile("<td class=\"lm\">(.+ \d+, \d+)\n"
                                    "<td class=\"rgt\">(.+)\n"
                                    "<td class=\"rgt\">(.+)\n"
                                    "<td class=\"rgt\">(.+)\n"
                                    "<td class=\"rgt\">(.+)\n"
                                    "<td class=\"rgt rm\">(.+)\n")

    def fetch_quotes_from_google(self):
        google_code = self.exchange.google_code
        if google_code is None:
            return

        r = requests.get(
            "https://finance.google.com/finance/historical?q=%s:%s&num=200"
            % (google_code, self.symbol))

        # <td class="lm">Apr 21, 2017
        # <td class="rgt">58.40
        # <td class="rgt">59.90
        # <td class="rgt">58.40
        # <td class="rgt">59.90
        # <td class="rgt rm">2,918

        for found in self._GOOGLE_FINANCE_RE.finditer(r.text):
            date = datetime.datetime.strptime(
                found.group(1), "%b %d, %Y").date()
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

    def fetch_quotes(self):
        """Get quotes from all available providers and merge them."""
        quotes_by_date = {}
        for func in self.QUOTES_PROVIDERS.values():
            for quote in func(self):
                quotes_by_date[quote.date] = quote
        return quotes_by_date

    def save_quotes(self):
        return storage.save(self.isin + "-quotes", self._quotes)

    def load_quotes(self):
        try:
            return storage.load(self.isin + "-quotes")
        except FileNotFoundError:
            return {}

    @property
    def quotes(self):
        if self._quotes is None:
            self._quotes = self.load_quotes()
            today = datetime.datetime.now().date()
            if not self._quotes or max(self._quotes.keys()) < today:
                self._quotes = self.fetch_quotes()
                self.save_quotes()
        return self._quotes
