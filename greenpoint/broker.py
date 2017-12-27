import datetime

import cachetools.func
import daiquiri
from lxml import html
import requests

from greenpoint import utils


LOG = daiquiri.getLogger(__name__)


TWO_YEARS = datetime.timedelta(days=365 * 2)


class Fortuneo(object):

    ACCESS_PAGE = "https://mabanque.fortuneo.fr/checkacces"
    HOME_PAGE = "https://mabanque.fortuneo.fr/fr/prive/default.jsp?ANav=1"
    PEA_HISTORY_PAGE = "https://mabanque.fortuneo.fr/fr/prive/mes-comptes/pea/historique/historique-titres.jsp?ca=%s"
    INSTRUMENT_SEARCH_PAGE = "https://www.fortuneo.fr/recherche?term=%s"

    def __init__(self, conf):
        self.session = requests.Session()
        login = self.session.post(self.ACCESS_PAGE,
                                  data={"login": conf['login'],
                                        "passwd": conf['password']})
        self.cookies = login.cookies
        home = self.session.get(self.HOME_PAGE, cookies=self.cookies)
        tree = html.fromstring(home.content)
        account_type = conf.get('account', '').lower()
        if account_type == 'pea':
            pea = tree.xpath('//div[@class="pea compte"]/a')
            if pea:
                self.pea_id = pea[0].get('rel')
            else:
                self.pea_id = None
        else:
            raise ValueError("No valid `account` specified in config")
        self.instruments = conf.get('instruments', {})

    @staticmethod
    def _translate_op(operation):
        op = operation.lower()
        if op in ("vente comptant", "rachat part sicav externe"):
            return "sell"
        elif op in ("achat comptant", "script-parts sicav externe"):
            return "buy"
        elif op.startswith("encaissement coupons"):
            return "dividend"
        elif op.startswith("taxe transac"):
            return "taxes"
        raise ValueError("Unknown transaction type")

    @staticmethod
    def _to_float(s):
        return float(s.replace(",", ".").replace("\xa0", ""))

    def list_transactions(self):
        return self._get_pea_history()

    @cachetools.func.ttl_cache(maxsize=4096, ttl=3600*24)
    def _get_instrument_info(self, instrument):
        LOG.debug("Fetching instrument %s", instrument)

        if instrument in self.instruments:
            LOG.debug("Instrument %s found in config", instrument)
            info = self.instruments[instrument]
            # If it's not a string, return the override
            if not isinstance(info, str):
                return info
            if info.startswith("http://") or info.startswith("https://"):
                url = info
            else:
                url = self.INSTRUMENT_SEARCH_PAGE % info
        else:
            url = self.INSTRUMENT_SEARCH_PAGE % instrument

        page = self.session.get(url)
        tree = html.fromstring(page.content)
        self.tree = tree
        try:
            caracts = tree.xpath(
                '//table[@class="caracteristics-values"]/tr/td/span/text()'
            )
            if caracts[1].strip() == "Action":
                type = "stock"
            else:
                raise ValueError("Unknown type %s" % caracts[1])
            pea = caracts[4].strip() == "oui"
            pea_pme = caracts[5].strip() == "oui"
            ttf = caracts[6].strip() == "oui"
            symbol, isin, exchange = map(
                lambda x: x.strip(),
                tree.xpath(
                    '//p[@class="digest-header-name-details"]/text()'
                )[0].split("-")
            )
        except (IndexError, ValueError):
            # ETF
            try:
                caracts = tree.xpath(
                    '//table[@class="caracteristics-values"][1]/tr/td/span/text()'
                )
                pea, pea_pme, ttf = map(
                    lambda x: x.strip().lower() == "oui",
                    caracts[8].split("/")
                )

                # isin = caracts[0]
                # exchange = caracts[1]
                if caracts[6].strip() == "ETF":
                    type = "ETF"
                else:
                    raise ValueError("Unknown type %s" % caracts[6])

                symbol, isin, exchange = map(
                    lambda x: x.strip(),
                    tree.xpath(
                        '//p[@class="digest-header-name-details"]/text()'
                    )[0].split("-")
                )
            except (ValueError, IndexError):
                # Mutual funds
                try:
                    cols = tree.xpath(
                        '//table[@class="caracteristics-values"]/tr/td/span/text()'
                    )
                    pea, pea_pme, fortuneo_vie = (cols[10].strip() == "oui",
                                                  cols[11].strip() == "oui",
                                                  cols[12].strip() == "oui")
                    isin, _ = map(
                        lambda x: x.strip(),
                        tree.xpath(
                            '//p[@class="digest-header-name-details"]/text()'
                        )[0].split("-")
                    )
                    exchange = None
                    ttf = False
                    symbol = None
                    type = "fund"
                except (ValueError, IndexError):
                    LOG.error("Unable to find info for %s", instrument)
                    return {"name": instrument}

        data = {
            "type": type,
            "name": instrument,
            "pea": pea,
            "pea_pme": pea_pme,
            "ttf": ttf,
            "symbol": symbol,
            "isin": isin,
            "exchange": exchange,
        }
        LOG.debug("Found info %s", data)
        return data

    def _get_pea_history(self):
        if self.pea_id is None:
            return []

        end = datetime.datetime.now()
        start = (end - TWO_YEARS)

        txs = []

        while True:
            page = self.session.post(
                self.PEA_HISTORY_PAGE % self.pea_id,
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

            for inst, op, xchange, date, qty, ppu, raw, fees, net, currency in map(
                    lambda t: tuple(map(lambda x: x.strip(), t)),
                    utils.grouper(history, 10)):
                try:
                    op = self._translate_op(op)
                except ValueError:
                    LOG.warning("Ignoring unknown tranaction type: %s", op)
                    continue

                qty = float(qty)

                if op == "taxes":
                    fees = self._to_float(raw)
                    ppu = self._to_float(ppu)
                elif op == "dividend":
                    ppu = self._to_float(net) / qty
                    # There is no fees, it's just the change, so use the net amount
                    # to get it
                    fees = 0
                else:
                    ppu = self._to_float(ppu)
                    fees = self._to_float(fees)

                txs.append({
                    "instrument": self._get_instrument_info(inst),
                    "operation": op,
                    "date": datetime.datetime.strptime(date, "%d/%m/%Y"),
                    "quantity": qty,
                    "price": ppu,
                    "fees": fees,
                    # Currency is always EUR anyway
                    "currency": "EUR",
                })

            end = start - datetime.timedelta(days=1)
            start = end - TWO_YEARS

        return txs


REGISTRY = {
    "fortuneo": Fortuneo,
}
