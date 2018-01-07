import datetime
import itertools
import operator

import attr
import enum

import collections

from greenpoint import instrument


class CashOperationType(enum.Enum):
    WITHDRAWAL = "withdrawal"
    DEPOSIT = "deposit"


@attr.s(frozen=True)
class CashOperation(object):
    type = attr.ib(validator=attr.validators.instance_of(CashOperationType))
    date = attr.ib(validator=attr.validators.instance_of(datetime.date))
    amount = attr.ib(validator=attr.validators.instance_of(float))
    currency = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(str)),
                       converter=attr.converters.optional(str.upper))


class OperationType(enum.Enum):
    BUY = "buy"
    SELL = "sell"
    DIVIDEND = "dividend"
    TAX = "tax"


@attr.s(frozen=True)
class Operation(object):
    instrument = attr.ib(validator=attr.validators.instance_of(
        instrument.Instrument))
    type = attr.ib(validator=attr.validators.instance_of(OperationType))
    date = attr.ib(validator=attr.validators.instance_of(datetime.date))
    quantity = attr.ib(validator=attr.validators.instance_of(float))
    price = attr.ib(validator=attr.validators.instance_of(float))
    fees = attr.ib(validator=attr.validators.instance_of(float))
    taxes = attr.ib(validator=attr.validators.instance_of(float))
    currency = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(str)),
                       converter=attr.converters.optional(str.upper))

    @property
    def amount(self):
        return self.quantity * self.price


@attr.s
class PortfolioInstrument(object):
    txs = attr.ib(validator=attr.validators.instance_of(list))
    instrument = attr.ib(
        validator=attr.validators.instance_of(
            instrument.Instrument),
        init=False)
    quantity = attr.ib(validator=attr.validators.instance_of(float),
                       init=False,
                       default=0.0)
    price = attr.ib(validator=attr.validators.instance_of(float),
                    init=False,
                    default=0.0)
    taxes = attr.ib(validator=attr.validators.instance_of(float),
                    init=False,
                    default=0.0)
    dividend = attr.ib(validator=attr.validators.instance_of(float),
                       init=False,
                       default=0.0)
    fees = attr.ib(validator=attr.validators.instance_of(float),
                   init=False,
                   default=0.0)
    gain = attr.ib(validator=attr.validators.instance_of(float),
                   init=False,
                   default=0.0)
    bought = attr.ib(validator=attr.validators.instance_of(float),
                     init=False,
                     default=0.0)
    sold = attr.ib(validator=attr.validators.instance_of(float),
                   init=False,
                   default=0.0)
    operations_count = attr.ib(
        validator=attr.validators.instance_of(dict),
        init=False,
        default=attr.Factory(lambda: collections.defaultdict(lambda: 0))
    )
    average_price_bought = attr.ib(
        validator=attr.validators.instance_of(float),
        init=False,
        default=0.0
    )
    average_price_sold = attr.ib(
        validator=attr.validators.instance_of(float),
        init=False,
        default=0.0,
    )
    date_first = attr.ib(validator=attr.validators.instance_of(datetime.date),
                         init=False)
    date_last = attr.ib(validator=attr.validators.instance_of(datetime.date),
                        init=False)

    @txs.validator
    def txs_validator(self, attribute, value):
        if not value:
            raise ValueError("At least one transaction is needed")
        instruments = [tx.instrument for tx in self.txs]
        instrument = instruments[0]
        if not all(instrument == inst for inst in instruments):
            raise ValueError(
                "Transactions are not all for the same instrument")

        currencies = [tx.currency for tx in self.txs]
        currency = currencies[0]
        if not all(currency == c for c in currencies):
            raise ValueError(
                "Transactions are not all in the same currency")

        self.currency = currency
        self.instrument = instrument

    def __attrs_post_init__(self):
        self.date_first = self.txs[0].date
        self.date_last = self.txs[0].date

        for tx in self.txs:
            self.date_first = min(self.date_first, tx.date)
            self.date_last = max(self.date_last, tx.date)
            self.fees += tx.fees
            self.taxes += tx.taxes
            self.operations_count[tx.type] += 1
            if tx.type == OperationType.BUY:
                total = self.quantity + tx.quantity
                if total != 0:
                    self.price = (self.price * self.quantity + tx.amount) / total
                    self.average_price_bought = (
                        self.average_price_bought * self.bought + tx.amount
                    ) / total
                self.quantity = total
                self.bought += tx.quantity
            elif tx.type == OperationType.SELL:
                self.quantity -= tx.quantity
                self.gain += (tx.price - self.price) * tx.quantity
                total = tx.quantity + self.sold
                if total != 0:
                    self.average_price_sold = (
                        (self.average_price_sold * self.sold) + tx.amount
                    ) / total
                self.sold += tx.quantity
            elif tx.type == OperationType.DIVIDEND:
                self.dividend += tx.amount

    def potential_gain(self, since=None):
        # FIXME(jd) currency conversion
        # if self.instrument.currency != self.currency:
        #     convert
        quote = self.instrument.quote
        if quote is None:
            return None
        if since is None:
            price = self.price
        else:
            try:
                previous_quote = self.instrument.quotes[
                    :quote.date - datetime.timedelta(days=1)
                ][-1]
            except KeyError:
                return None
            price = previous_quote.close
        return (quote.close - price) * self.quantity


ATTRGETTER_INSTRUMENT = operator.attrgetter('instrument')


def _get_class_name(o):
    return type(o).__name__


@attr.s(frozen=True)
class Portfolio(object):

    txs = attr.ib(validator=attr.validators.instance_of(
        list))

    def get_portfolio(self, date=None):
        instruments = []
        currencies = collections.defaultdict(lambda: 0)

        for type_, txs in itertools.groupby(
                sorted(self.txs, key=_get_class_name),
                key=_get_class_name):

            if type_ == 'CashOperation':
                for tx in txs:
                    if date is None or tx.date <= date:
                        currencies[tx.currency] += tx.amount

            elif type_ == 'Operation':
                for _, gtxs in itertools.groupby(
                        sorted((tx for tx in txs
                                if date is None or tx.date <= date),
                               key=ATTRGETTER_INSTRUMENT),
                        key=ATTRGETTER_INSTRUMENT):
                    instruments.append(PortfolioInstrument(list(gtxs)))
            else:
                raise RuntimeError("Unknown operation type")

        return instruments, currencies
