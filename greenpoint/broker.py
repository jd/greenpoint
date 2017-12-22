import enum
import collections
import datetime

import daiquiri
from lxml import html
import requests

from greenpoint import utils


LOG = daiquiri.getLogger(__name__)


TWO_YEARS = datetime.timedelta(days=365 * 2)


class TransactionOperation(enum.Enum):
    BUY = 0
    SELL = 1
    DIVIDEND = 2
    TAXES = 3


Transaction = collections.namedtuple('Transaction', [
    "instrument",
    "operation",
    "date",
    "quantity",
    "price",
    "fees",
])


class Fortuneo(object):

    ACCESS_PAGE = "https://mabanque.fortuneo.fr/checkacces"
    PEA_HISTORY_PAGE = "https://mabanque.fortuneo.fr/fr/prive/mes-comptes/pea/historique/historique-titres.jsp?ca=%s"

    def __init__(self, login, password, pea_id=None):
        self.session = requests.Session()
        login = self.session.post(self.ACCESS_PAGE,
                                  data={"login": login, "passwd": password})
        self.cookies = login.cookies
        # TODO(jd) Find PEA automatically by parsing login page
        self.pea_id = pea_id

    @staticmethod
    def _translate_op(operation):
        op = operation.lower()
        if op in ("vente comptant", "rachat part sicav externe"):
            return TransactionOperation.SELL
        elif op in ("achat comptant", "script-parts sicav externe"):
            return TransactionOperation.BUY
        elif op.startswith("encaissement coupons"):
            return TransactionOperation.DIVIDEND
        elif op.startswith("taxe transac"):
            return TransactionOperation.TAXES
        raise ValueError("Unknown transaction type")

    @staticmethod
    def _to_float(s):
        return float(s.replace(",", ".").replace("\xa0", ""))

    def get_pea_history(self):
        if self.pea_id is None:
            return

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
                '//table[@id="tabHistoriqueOperations"]/tbody/tr/td')

            if len(history) == 0:
                break

            for inst, op, xchange, date, qty, ppu, raw, fees, net, currency in map(lambda t: tuple(map(lambda x: x.text.strip(), t)),
                                                                                   utils.grouper(history, 10)):
                try:
                    op = self._translate_op(op)
                except ValueError:
                    LOG.warning("Ignoring unknown tranaction type: %s", op)
                    continue

                qty = int(qty)

                if op == TransactionOperation.TAXES:
                    fees = self._to_float(raw)
                    ppu = self._to_float(ppu)
                elif op == TransactionOperation.DIVIDEND:
                    ppu = self._to_float(net) / qty
                    # There is no fees, it's just the change, so use the net amount
                    # to get it
                    fees = 0
                else:
                    ppu = self._to_float(ppu)
                    fees = self._to_float(fees)

                txs.append(Transaction(
                    # FIXME(jd) Normalize + xchange,
                    inst, op,
                    datetime.datetime.strptime(date, "%d/%m/%Y"),
                    qty, ppu, fees))

            end = start - datetime.timedelta(days=1)
            start = end - TWO_YEARS

        return txs
