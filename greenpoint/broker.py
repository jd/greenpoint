import datetime

import cachetools.func

import daiquiri

from lxml import html

import requests

from greenpoint import portfolio
from greenpoint import utils


LOG = daiquiri.getLogger(__name__)


TWO_YEARS = datetime.timedelta(days=365 * 2)


class Fortuneo(object):

    ACCESS_PAGE = "https://mabanque.fortuneo.fr/checkacces"
    HOME_PAGE = "https://mabanque.fortuneo.fr/fr/prive/default.jsp?ANav=1"

    HISTORY_PAGE = ("https://mabanque.fortuneo.fr/fr/prive/mes-comptes/%s"
                    "/historique/historique-titres.jsp?ca=%s")

    INSTRUMENT_SEARCH_PAGE = "https://www.fortuneo.fr/recherche?term=%s"

    INSTRUMENTS = {
        "KERLINK": "https://bourse.fortuneo.fr/actions/cours-kerlink-ALKLK-FR0013156007-23",
        "BASTIDE LE CONFORT": "https://bourse.fortuneo.fr/actions/cours-bastide-le-confort-BLC-FR0000035370-23",
        "LYXOR UCITS ETF Eastern Europe (CECE NTR EUR)": "https://bourse.fortuneo.fr/trackers/cours-lyxor-ucits-etf-eastern-europe-cece-ntr-eur-CEC-FR0010204073-23",
        "AUBAY": "https://bourse.fortuneo.fr/actions/cours-aubay-AUB-FR0000063737-23",
        "AMUNDI ETF MSCI EMERGING MARKETS UCITS ETF": "https://bourse.fortuneo.fr/trackers/cours-amundi-etf-msci-emerging-markets-ucits-etf-AEEM-FR0010959676-23",
        "AMUNDI ETF S&P 500 UCITS ETF": "https://bourse.fortuneo.fr/trackers/cours-amundi-etf-s-p-500-ucits-etf-500-FR0010892224-23",
        "L'OREAL": "https://bourse.fortuneo.fr/actions/cours-l-oreal-OR-FR0000120321-23",
        "SANOFI": "FR0000120578",
        "LVMH": "FR0000121014",
        "OCTO TECHNOLOGY": portfolio.Instrument(
            isin="FR0004157428",
            type=portfolio.InstrumentType.STOCK,
            name="Octo Technology",
            pea=True,
            pea_pme=True,
            ttf=False,
            symbol="ALOCT",
            exchange="Euronext Paris",
        ),
        "KERLINK DS": portfolio.Instrument(
            isin="FR0013251287",
            type=portfolio.InstrumentType.STOCK,
            name="Kerlink DS",
            pea=False,
            pea_pme=False,
            ttf=False,
            symbol="KLKDS",
            exchange="Euronext Paris",
        ),
        "ILIAD": "FR0004035913",
        "DIRECT ENERGIE": "FR0004191674",
        "ROYAL DUTCH SHELLB": "FTN000046GB00B03MM408",
        "Hsbc Small Cap France A A/i": "https://bourse.fortuneo.fr/sicav-fonds/cours-hsbc-small-cap-france-a-a-i-FR0010058628-26",
        "Federal Indiciel Us P A/i": "https://bourse.fortuneo.fr/sicav-fonds/cours-federal-indiciel-us-p-a-i-FR0000988057-26",
        "BNP Paribas Easy MSCI Europe Small Caps ex Controversial Weapons UCITS ETF Capitalisation": "LU1291101555",
        "Ensco 'A'": "https://bourse.fortuneo.fr/actions/cours-ensco-a-ESV-GB00B4VLR192-75",
    }

    def __init__(self, conf):
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
        if self.account_type in ('pea', 'ppe'):
            account_id = tree.xpath(
                '//div[@class="%s compte"]/a' % self.account_type
            )[0].get('rel')
            self.cash_history_page = (
                "https://mabanque.fortuneo.fr/fr/prive/mes-comptes/%s/"
                "historique/historique-especes.jsp?ca=%s" % (
                    self.account_type, account_id
                )
            )
            self.history_page = self.HISTORY_PAGE % (self.account_type,
                                                     account_id)
        elif self.account_type == 'ord':
            account_esp_id = tree.xpath(
                '//div[@class="esp compte"]/a')[0].get('rel')
            account_id = tree.xpath(
                '//div[@class="ord compte"]/a')[0].get('rel')
            self.cash_history_page = (
                "https://mabanque.fortuneo.fr/fr/prive/mes-comptes/"
                "compte-especes/consulter-situation/consulter-solde.jsp?ca=%s"
                % account_esp_id
            )
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
            return "dividend"
        elif op.startswith("taxe transac"):
            return "taxes"
        raise ValueError("Unknown transaction type")

    @staticmethod
    def _to_float(s):
        return float(s.replace(",", ".").replace("\xa0", ""))

    @cachetools.func.ttl_cache(maxsize=4096, ttl=3600 * 24)
    def _get_instrument_info(self, instrument):
        LOG.debug("Fetching instrument %s", instrument)

        info = self.INSTRUMENTS.get(instrument)
        if info:
            LOG.debug("Instrument %s pre-configured", instrument)
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

        instrument_kwargs = {"name": instrument}

        if page.url.startswith("https://bourse.fortuneo.fr/actions/"):
            caracts = tree.xpath(
                '//table[@class="caracteristics-values"]/tr/td/span/text()'
            )
            if caracts[1].strip() == "Action":
                instrument_kwargs['type'] = portfolio.InstrumentType.STOCK
            else:
                raise ValueError("Unknown type %s" % caracts[1])
            instrument_kwargs["pea"] = caracts[4].strip() == "oui"
            instrument_kwargs["pea_pme"] = caracts[5].strip() == "oui"
            instrument_kwargs["ttf"] = caracts[6].strip() == "oui"

            (instrument_kwargs['symbol'],
             instrument_kwargs['isin'],
             instrument_kwargs['exchange']) = map(
                lambda x: x.strip(),
                tree.xpath(
                    '//p[@class="digest-header-name-details"]/text()'
                )[0].split("-")
            )
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
                instrument_kwargs['type'] = portfolio.InstrumentType.ETF
            else:
                raise ValueError("Unknown type %s" % caracts[6])

            (instrument_kwargs['symbol'],
             instrument_kwargs['isin'],
             instrument_kwargs['exchange']) = map(
                lambda x: x.strip(),
                tree.xpath(
                    '//p[@class="digest-header-name-details"]/text()'
                )[0].split("-")
            )
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
            instrument_kwargs['type'] = portfolio.InstrumentType.FUND
            instrument_kwargs['symbol'] = None
            instrument_kwargs['exchange'] = None
        else:
            raise RuntimeError("Unable to find info for %s", instrument)

        data = portfolio.Instrument(**instrument_kwargs)
        LOG.debug("Found info %s", data)
        return data

    def list_transactions(self):
        end = datetime.datetime.now()
        start = (end - TWO_YEARS)

        txs = []

        while True:
            if self.account_type == "ord":
                page = self.session.post(
                    self.cash_history_page,
                    data={
                        "dateRechercheDebut": start.strftime("%d/%m/%Y"),
                        "dateRechercheFin": end.strftime("%d/%m/%Y"),
                        "nbrEltsParPage": 1000,
                    },
                    cookies=self.cookies)
            else:
                page = self.session.post(
                    self.cash_history_page,
                    data={
                        "offset": 0,
                        "dateDebut": start.strftime("%d/%m/%Y"),
                        "dateFin": end.strftime("%d/%m/%Y"),
                        "nbResultats": 1000,
                    },
                    cookies=self.cookies)

            tree = html.fromstring(page.content)

            history = tree.xpath(
                '//table[@id="tabHistoriqueOperations"]/tbody/tr/td')

            if len(history) == 0:
                break

            for _, date_op, date_value, label, debit, credit in map(
                    lambda t: tuple(map(lambda x: x.text.strip(), t)),
                    utils.grouper(history, 6)):
                if credit:
                    amount = self._to_float(credit)
                    operation = "deposit"
                else:
                    amount = self._to_float(debit)
                    operation = "withdrawal"
                txs.append({
                    "instrument": None,
                    "operation": operation,
                    "date": datetime.datetime.strptime(
                        date_op, "%d/%m/%Y").date(),
                    "amount": amount,
                    # Currency is always EUR anyway
                    "currency": "EUR",
                })
            end = start - datetime.timedelta(days=1)
            start = end - TWO_YEARS

        end = datetime.datetime.now()
        start = (end - TWO_YEARS)

        while True:
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
                try:
                    op = self._translate_op(op)
                except ValueError:
                    LOG.warning("Ignoring unknown tranaction type: %s", op)
                    continue

                qty = self._to_float(qty)
                taxes = 0

                if op == "dividend":
                    ppu = self._to_float(net) / qty
                    # There is no fees, it's just the change, so use the net
                    # amount to get it
                    fees = 0
                elif op == "taxes":
                    # FIXME(jd) should be sell/buy since it's TTF and included
                    # in the previous transaction
                    taxes = self._to_float(fees)
                    fees = 0
                    ppu = 0
                else:
                    if currency != "EUR":
                        ppu = abs(self._to_float(net)) / qty
                        # Fees is change + fees…
                        fees = 0
                    else:
                        ppu = self._to_float(ppu)
                        fees = self._to_float(fees)

                txs.append({
                    "instrument": self._get_instrument_info(inst),
                    "operation": op,
                    "date": datetime.datetime.strptime(
                        date, "%d/%m/%Y").date(),
                    "quantity": qty,
                    "price": ppu,
                    "fees": fees,
                    "taxes": taxes,
                    # Currency is always EUR anyway
                    "currency": "EUR",
                })

            end = start - datetime.timedelta(days=1)
            start = end - TWO_YEARS

        return txs


REGISTRY = {
    "fortuneo": Fortuneo,
}
