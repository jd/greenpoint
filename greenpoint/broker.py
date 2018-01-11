import datetime
import os.path

import cachetools

import daiquiri

from lxml import html

import requests

import yaml

from greenpoint import instrument
from greenpoint import portfolio
from greenpoint import utils


LOG = daiquiri.getLogger(__name__)


ONE_YEAR = datetime.timedelta(days=365)


class Fortuneo(object):

    ACCESS_PAGE = "https://mabanque.fortuneo.fr/checkacces"
    HOME_PAGE = "https://mabanque.fortuneo.fr/fr/prive/default.jsp?ANav=1"

    HISTORY_PAGE = ("https://mabanque.fortuneo.fr/fr/prive/mes-comptes/%s"
                    "/historique/historique-titres.jsp?ca=%s")

    INSTRUMENT_SEARCH_PAGE = "https://www.fortuneo.fr/recherche?term=%s"

    with open(os.path.join(os.path.dirname(__file__),
                           "data", "fortuneo.yaml"), "r") as f:
        PRELOAD = yaml.load(f.read())

    CACHE = cachetools.TTLCache(maxsize=4096, ttl=3600 * 24)

    def __init__(self, name, conf):
        self.name = name
        self.session = requests.Session()
        login = self.session.post(self.ACCESS_PAGE,
                                  data={"login": conf['login'],
                                        "passwd": conf['password']})
        self.cookies = login.cookies
        home = self.session.get(self.HOME_PAGE, cookies=self.cookies)
        tree = html.fromstring(home.content)
        self.account_type = conf.get('account', '').lower()
        if self.account_type == 'pea-pme':
            self.account_type = 'ppe'  # Fortuneo name
        account_id = tree.xpath(
            '//div[@class="%s compte"]/a' % self.account_type
        )[0].get('rel')
        if self.account_type in ('pea', 'ppe'):
            self.history_page = self.HISTORY_PAGE % (self.account_type,
                                                     account_id)
        elif self.account_type == 'ord':
            self.history_page = self.HISTORY_PAGE % ("compte-titres-pea",
                                                     account_id)
        else:
            raise ValueError("No valid `account` specified in config")

    @staticmethod
    def _translate_op(operation):
        op = operation.lower()
        if op in ("vente comptant", "rachat part sicav externe"):
            return "sell"
        elif op in ("achat comptant", "script-parts sicav externe",
                    "Dépôt de titres vifs"):
            return "buy"
        elif op.startswith("encaissement coupons"):
            return portfolio.OperationType.DIVIDEND
        elif op.startswith("taxe transac"):
            return portfolio.OperationType.TAX
        elif (op.startswith("ost de création de coupons") or
              op.startswith("annul. ost de création de coupons") or
              op.startswith("conversion forme de titre") or
              op.startswith("ost d information")):
            return
        LOG.error("Unknown transaction type `%s'", operation)

    @staticmethod
    def _to_float(s):
        return float(s.replace(",", ".").replace("\xa0", ""))

    _EXCHANGE_MAP = {
        "euronext paris": "XPAR",
        "euronext bruxelles": "XBRU",
        "new york stock exchange": "XNYS",
        "lse foreign currency": "XLON",
        "euronext amsterdam": "XAMS",
    }

    @classmethod
    async def _get_instrument_info(cls, session, name):
        # Use a decorator
        # https://github.com/tkem/cachetools/issues/92

        if name in cls.CACHE:
            return cls.CACHE[name]

        LOG.debug("Fetching instrument %s", name)

        info = cls.PRELOAD.get('instruments', {}).get(name)
        if info:
            LOG.debug("Instrument %s pre-configured", name)
            # If it's not a string, return the override
            if not isinstance(info, str):
                return await instrument.Instrument.load(**info)
            if info.startswith("http://") or info.startswith("https://"):
                url = info
            else:
                url = cls.INSTRUMENT_SEARCH_PAGE % info
        else:
            url = cls.INSTRUMENT_SEARCH_PAGE % name

        page = session.get(url)
        tree = html.fromstring(page.content)

        instrument_kwargs = {"name": name}

        if page.url.startswith("https://bourse.fortuneo.fr/actions/"):
            caracts = tree.xpath(
                '//table[@class="caracteristics-values"]/tr/td/span/text()'
            )
            if caracts[1].strip() == "Action":
                instrument_kwargs['type'] = instrument.InstrumentType.STOCK
            else:
                raise ValueError("Unknown type %s" % caracts[1])
            instrument_kwargs["pea"] = caracts[4].strip() == "oui"
            instrument_kwargs["pea_pme"] = caracts[5].strip() == "oui"
            instrument_kwargs["ttf"] = caracts[6].strip() == "oui"

            (instrument_kwargs['symbol'],
             instrument_kwargs['isin'],
             exchange) = map(
                lambda x: x.strip(),
                tree.xpath(
                    '//p[@class="digest-header-name-details"]/text()'
                )[0].split("-")
            )
            instrument_kwargs['exchange_mic'] = cls._EXCHANGE_MAP[
                exchange.lower()]
        elif page.url.startswith("https://bourse.fortuneo.fr/trackers/"):
            # ETF
            caracts = tree.xpath(
                '//table[@class="caracteristics-values"][1]/tr/td/span/text()'
            )
            (instrument_kwargs['pea'],
             instrument_kwargs['pea_pme'],
             instrument_kwargs['ttf']) = map(
                lambda x: x.strip().lower() == "oui",
                caracts[8].split("/")
            )

            # isin = caracts[0]
            # exchange = caracts[1]
            if caracts[6].strip() == "ETF":
                instrument_kwargs['type'] = instrument.InstrumentType.ETF
            else:
                raise ValueError("Unknown type %s" % caracts[6])

            (instrument_kwargs['symbol'],
             instrument_kwargs['isin'],
             exchange) = map(
                lambda x: x.strip(),
                tree.xpath(
                    '//p[@class="digest-header-name-details"]/text()'
                )[0].split("-")
            )
            instrument_kwargs['exchange_mic'] = cls._EXCHANGE_MAP[
                exchange.lower()]
        elif page.url.startswith("https://bourse.fortuneo.fr/sicav-fonds/"):
            # Mutual funds
            cols = tree.xpath(
                '//table[@class="caracteristics-values"]/tr/td/span/text()'
            )
            (instrument_kwargs['pea'],
             instrument_kwargs['pea_pme']) = (cols[10].strip() == "oui",
                                              cols[11].strip() == "oui")
            instrument_kwargs['isin'], _ = map(
                lambda x: x.strip(),
                tree.xpath(
                    '//p[@class="digest-header-name-details"]/text()'
                )[0].split("-")
            )
            instrument_kwargs['ttf'] = False
            instrument_kwargs['type'] = instrument.InstrumentType.FUND
            instrument_kwargs['symbol'] = None
            instrument_kwargs['exchange_mic'] = None
        else:
            raise ValueError("Unable to find info for %s", name)

        instrument_kwargs['currency'] = tree.xpath(
            '//div[@class="digest-header-number"]/span/text()'
        )[0].rsplit(" ", 1)[-1]

        data = await instrument.Instrument.load(**instrument_kwargs)
        LOG.debug("Found info %s", data)
        cls.CACHE[name] = data
        return data

    @staticmethod
    def _iter_on_time(step=ONE_YEAR):
        end = datetime.datetime.now()
        start = (end - step)
        while True:
            yield (start, end)
            end = start - datetime.timedelta(days=1)
            start = end - step

    async def list_transactions(self):
        txs = []

        for start, end in self._iter_on_time():
            page = self.session.post(
                self.history_page,
                data={
                    "offset": 0,
                    "dateDebut": start.strftime("%d/%m/%Y"),
                    "dateFin": end.strftime("%d/%m/%Y"),
                    "nbResultats": 1000,
                },
                cookies=self.cookies)

            tree = html.fromstring(page.content)
            history = tree.xpath(
                '//table[@id="tabHistoriqueOperations"]/tbody/tr/td/text()')

            if len(history) == 0:
                break

            for inst, op, xchange, date, qty, ppu, raw, fees, net, currency in map(  # noqa
                    lambda t: tuple(map(lambda x: x.strip(), t)),
                    utils.grouper(history, 10)):

                op = self._translate_op(op)
                if op is None:
                    continue

                qty = self._to_float(qty)

                if op in ("buy", "sell"):
                    if op == "sell":
                        qty = - qty
                    op = portfolio.OperationType.TRADE

                taxes = 0.0
                final_fees = 0.0

                if op == portfolio.OperationType.DIVIDEND:
                    if currency == "EUR":
                        # Fees are taxes actually
                        taxes = self._to_float(fees)
                        ppu = abs(self._to_float(raw)) / qty
                    else:
                        # There is no fees, it's just the change and
                        # prelevement a la source sometimes, so use the net
                        # amount to get something interesting
                        ppu = abs(self._to_float(net)) / qty
                elif op == portfolio.OperationType.TAX:
                    taxes = self._to_float(fees)
                    ppu = 0.0
                else:
                    if currency != "EUR":
                        # Fees is change + fees… ignore
                        ppu = abs(self._to_float(net)) / qty
                    else:
                        ppu = self._to_float(ppu)
                        final_fees = self._to_float(fees)

                try:
                    inst = await self._get_instrument_info(self.session, inst)
                except ValueError:
                    LOG.warning("Ignoring unknown instrument `%s'", inst)
                    continue

                txs.append(portfolio.Operation(
                    instrument_isin=inst.isin,
                    type=op,
                    date=datetime.datetime.strptime(
                        date, "%d/%m/%Y").date(),
                    quantity=qty,
                    price=ppu,
                    fees=final_fees,
                    taxes=taxes,
                    # Currency is always EUR anyway
                    currency="EUR",
                ))

        return txs


REGISTRY = {
    "fortuneo": Fortuneo,
}
