import datetime

import attr
import enum

import collections


class InstrumentType(enum.Enum):
    ETF = "etf"
    STOCK = "stock"
    FUND = "fund"


@attr.s(frozen=True)
class Instrument(object):
    isin = attr.ib(validator=attr.validators.instance_of(str),
                   converter=str.upper)
    type = attr.ib(validator=attr.validators.instance_of(InstrumentType),
                   cmp=False)
    name = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(str)), cmp=False)
    symbol = attr.ib(validator=attr.validators.optional(
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
        attr.validators.instance_of(str)), cmp=False)


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
    instrument = attr.ib(validator=attr.validators.instance_of(Instrument))
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


def _default_instrument():
    return {
        "price": 0,
        "quantity": 0,
        "dividend": 0,
        "taxes": 0,
        "fees": 0,
        "gain": 0,
        "trades": 0,
        "bought": 0,
        "sold": 0,
        "average_price_bought": 0,
        "average_price_sold": 0,
    }


def get_portfolio(txs, date=None):
    instruments = collections.defaultdict(_default_instrument)
    currencies = collections.defaultdict(lambda: 0)

    for tx in txs:
        if date is not None and tx.date > date:
            continue

        if isinstance(tx, CashOperation):
            currencies[tx.currency] += tx.amount
            continue

        instrument = instruments[tx.instrument]
        instrument["fees"] += tx.fees
        instrument["taxes"] += tx.taxes
        if tx.type == OperationType.BUY:
            total = (instrument["quantity"] + tx.quantity)
            if total != 0:
                instrument["price"] = (
                    instrument["price"] * instrument["quantity"] +
                    tx.amount
                ) / total
                instrument["average_price_bought"] = (
                    (instrument["average_price_bought"] *
                     instrument["bought"]) +
                    tx.amount
                ) / total
            instrument["quantity"] = total
            instrument["trades"] += 1
            instrument["bought"] += tx.quantity
        elif tx.type == OperationType.SELL:
            instrument["quantity"] -= tx.quantity
            instrument["trades"] += 1
            instrument["gain"] += ((tx.price - instrument["price"]) *
                                   tx.quantity)
            total = tx.quantity + instrument["sold"]
            if total != 0:
                instrument["average_price_sold"] = (
                    (instrument["average_price_sold"] * instrument["sold"]) +
                    tx.amount
                ) / total
            instrument["sold"] += tx.quantity
        elif tx.type == OperationType.DIVIDEND:
            instrument["dividend"] += tx.amount

    return instruments, currencies
