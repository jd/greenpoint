import asyncio
import datetime
import itertools
import json
import re

import aiohttp

import attr

import enum

import daiquiri

import iso8601

from lxml import etree

from greenpoint import utils


LOG = daiquiri.getLogger(__name__)

ONE_DAY = datetime.timedelta(days=1)


@attr.s(slots=True, frozen=True)
class Quote(object):
    date = attr.ib(validator=attr.validators.instance_of(datetime.date))
    open = attr.ib(validator=attr.validators.optional(  # noqa
        attr.validators.instance_of(float)), hash=False)
    close = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(float)), hash=False)
    high = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(float)), hash=False)
    low = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(float)), hash=False)
    volume = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(int)), hash=False)


class InstrumentType(enum.Enum):
    ETF = "etf"
    STOCK = "stock"
    FUND = "fund"

    @classmethod
    def strtoenum(cls, value):
        if isinstance(value, cls):
            return value
        return cls[value.upper()]


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


YAHOO_MIC_MAP = {
    "XPAR": ".PA",
    "XBRU": ".BR",
    "XAMS": ".AS",
    "XLON": ".L",
    "XNYS": "",
}


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
    exchange_mic = attr.ib(
        validator=attr.validators.optional(
            attr.validators.instance_of(str)),
        converter=attr.converters.optional(str.upper),
        cmp=False)
    currency = attr.ib(
        validator=attr.validators.optional(
            attr.validators.instance_of(str)),
        converter=attr.converters.optional(str.upper))
    latest_quote = attr.ib(
        validator=attr.validators.optional(
            attr.validators.instance_of(float)),
        default=None)
    latest_quote_time = attr.ib(
        validator=attr.validators.optional(
            attr.validators.instance_of(datetime.datetime)),
        default=None)
    is_alive = attr.ib(
        validator=attr.validators.instance_of(bool),
        default=True)

    def __str__(self):
        if self.symbol:
            return "<%s%s: %s (%s)>" % (
                self.symbol,
                ("@" + self.exchange_mic) if self.exchange_mic else "",
                self.name,
                self.type.name)
        else:
            return "<%s (%s)>" % (self.name, self.type.name)

    @property
    def google_symbol(self):
        if self.exchange_mic is None:
            return
        google_code = GOOGLE_MIC_MAP.get(self.exchange_mic)
        if google_code is None:
            return
        return google_code + ":" + self.symbol

    @property
    def yahoo_symbol(self):
        if self.exchange_mic is None:
            if self.type == InstrumentType.FUND:
                # NOTE This is probably wrong, but currently we only support
                # fund that are traded in France, so that works for Yahoo.
                return self.isin + ".PA"
            else:
                return
        yahoo_code = YAHOO_MIC_MAP.get(self.exchange_mic)
        if yahoo_code is None:
            return
        return self.symbol + yahoo_code

    async def save(self):
        cur = await utils.get_db()
        await cur.execute(
            "INSERT INTO instruments "
            "(isin, name, type, symbol, pea, pea_pme, ttf, "
            "exchange_mic, currency) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
            "ON CONFLICT ON CONSTRAINT instruments_pkey "
            "DO NOTHING",
            self.isin, self.name, self.type.name.lower(),
            self.symbol, self.pea, self.pea_pme, self.ttf, self.exchange_mic,
            self.currency,
        )

    @classmethod
    async def load(cls, **kwargs):
        cur = await utils.get_db()
        if 'isin' in kwargs:
            result = await cur.fetchrow(
                "SELECT * FROM instruments WHERE isin = $1",
                kwargs['isin'])
        elif 'name' in kwargs:
            result = await cur.fetchrow(
                "SELECT * FROM instruments WHERE name ILIKE $1",
                "%" + kwargs['name'] + "%")
        else:
            result = None

        if result:
            return cls(**result)

        i = cls(**kwargs)
        await i.save()
        return i

    async def fetch_quotes_from_boursorama(self, session,
                                           start=None, stop=None):
        quotes = set()
        async with session.get(
                "http://www.boursorama.com/recherche/index.phtml?q=" +
                self.isin) as r:
            try:
                symbol = str(r.url).split("symbole=")[1]
            except IndexError:
                return quotes

        async with session.get(
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
        ) as r:
            # NOTE Content-Type is wrong, so cannot use r.json() here
            content = await r.read()
            json = json.loads(content)
            for point in json['dataSets'][0]['dataProvider']:
                d = datetime.datetime.strptime(
                    point['d'][:-6], "%d/%m/%Y").date()
                if start is not None and d < start:
                    continue
                if stop is not None and d > stop:
                    continue
                quotes.add(Quote(
                    date=d,
                    open=float(point['o']),
                    close=float(point['c']),
                    high=float(point['h']),
                    low=float(point['l']),
                    volume=point['v']
                ))

        return quotes

    async def fetch_quotes_from_lesechos(self, session, start=None, stop=None):
        quotes = set()
        exchange = self.exchange_mic
        if not exchange:
            if self.type == InstrumentType.FUND:
                exchange = "WMORN"  # Morningstar fund
            else:
                return quotes

        if start is None:
            start = datetime.date(2000, 1, 1)
        if stop is None:
            stop = datetime.datetime.now().date()

        start = start.strftime("%Y%m%d")
        stop = stop.strftime("%Y%m%d")

        async with session.get(
                "https://lesechos-bourse-fo-cdn.wlb.aw.atos.net" +
                "/FDS/history.xml?entity=echos&view=ALL" +
                "&code=" + self.isin +
                "&codification=ISIN&adjusted=true&base100=false" +
                "&exchange=" + exchange +
                "&sessWithNoQuot=false" +
                "&beginDate=" + start +
                "&endDate=" + stop +
                "&computeVar=true") as r:
            content = await r.read()

        xml = etree.fromstring(content)
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
                if v is None:
                    break
                v = float(v)
                if kwarg == "volume":
                    v = int(v)
                kwargs[kwarg] = v
            else:
                quotes.add(Quote(**kwargs))
        return quotes

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

    async def fetch_quotes_from_google(self, session, start=None, stop=None):
        quotes = set()
        google_symbol = self.google_symbol
        if google_symbol is None:
            LOG.warning("No Google code for %r, cannot fetch quotes", self)
            return quotes

        for index in itertools.count(0, 200):
            async with session.get(
                    "https://finance.google.com/finance/historical"
                    "?q=%s&num=200&start=%d"
                    % (google_symbol, index)) as r:
                text = await r.text()

            results = list(self._GOOGLE_FINANCE_RE.finditer(text))
            if not len(results):
                return quotes

            for found in results:
                date = datetime.datetime.strptime(
                    found.group(1), "%b %d, %Y").date()
                if start is not None and date < start:
                    # Results are ordered descending
                    # As soon as a date is before the start, we can stop
                    break
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
                    quotes.add(
                        Quote(
                            date=date,
                            open=values[0],
                            high=values[1],
                            low=values[2],
                            close=values[3],
                            volume=int(values[4]),
                        )
                    )

        return quotes

    QUOTES_PROVIDERS = {
        "boursorama": fetch_quotes_from_boursorama,
        "lesechos": fetch_quotes_from_lesechos,
        "google": fetch_quotes_from_google,
    }

    async def refresh_quotes(self, start=None, stop=None):
        """Get quotes from all available providers and merge them.

        :param start: Timestamp to start at (included)
        :param stop: Timestamp to stop at (included)
        """
        cur = await utils.get_db()
        new_quotes = set()
        async with aiohttp.ClientSession() as session:
            futs = [asyncio.ensure_future(func(self, session, start, stop))
                    for func in self.QUOTES_PROVIDERS.values()]
            for fut in futs:
                new_quotes.update(await asyncio.wait_for(fut, None))
        await cur.executemany(
            "INSERT INTO quotes "
            "(instrument_isin, date, open, close, high, low, volume) "
            "VALUES($1, $2, $3, $4, $5, $6, $7) "
            "ON CONFLICT ON CONSTRAINT quotes_instrument_isin_date_key "
            "DO UPDATE SET "
            "open = COALESCE(quotes.open, excluded.open), "
            "close = COALESCE(quotes.close, excluded.close), "
            "high = COALESCE(quotes.high, excluded.high), "
            "low = COALESCE(quotes.low, excluded.low), "
            "volume = COALESCE(quotes.volume, excluded.volume)",
            ((self.isin, quote.date, quote.open, quote.close,
              quote.high, quote.low, quote.volume)
             for quote in new_quotes),
        )

    async def refresh_live_quote(self):
        async with aiohttp.ClientSession() as session:
            ret = await self.fetch_live_quote_from_yahoo(session)
            try:
                ts, quote = ret
            except TypeError:
                LOG.info("Unable to find live quote for %s", self)
                return
            conn = await utils.get_db()
            await conn.execute(
                "UPDATE instruments "
                "SET latest_quote = $1, latest_quote_time = $2 "
                "WHERE isin = $3",
                quote, ts, self.isin)

    async def fetch_live_quote_from_yahoo(self, session):
        yahoo_symbol = self.yahoo_symbol
        if yahoo_symbol is None:
            LOG.warning("No Yahoo code for %s, cannot fetch quotes", self)
            return

        async with session.get(
                "https://query1.finance.yahoo.com/v7/finance/quote?"
                "lang=en-US&region=US&corsDomain=finance.yahoo.com"
                "&fields=currency,"
                "regularMarketDayHigh,"
                "regularMarketDayLow,"
                "regularMarketOpen,"
                "regularMarketPrice,"
                "regularMarketTime,"
                "regularMarketVolume"
                "&symbols=" + yahoo_symbol) as r:
            json = await r.json()

        result = json['quoteResponse']['result']
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
        if 'regularMarketOpen' in result:
            return (datetime.datetime.utcfromtimestamp(
                result['regularMarketTime']).replace(tzinfo=iso8601.UTC),
                    result['regularMarketPrice'])

        # Not used anymore because Quote wants a date and we use datetime
        # return Quote(
        #     date=datetime.datetime.utcfromtimestamp(
        #         result['regularMarketTime']).date(),
        #     open=result['regularMarketOpen'],
        #     close=result['regularMarketPrice'],
        #     low=result['regularMarketDayLow'],
        #     high=result['regularMarketDayHigh'],
        #     volume=result['regularMarketVolume'],
        # )

    @classmethod
    async def list_instruments(cls):
        cur = await utils.get_db()
        rows = await cur.fetch("SELECT * FROM instruments")
        return [cls(**row) for row in rows]
