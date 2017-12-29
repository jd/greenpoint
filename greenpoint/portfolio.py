import collections
import operator


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
        if tx["operation"] in ("deposit", "withdrawal"):
            currencies[tx['currency']] += tx['amount']
            continue

        key = tuple(sorted(tx["instrument"].items(),
                           key=operator.itemgetter(0)))
        instrument = instruments[key]
        taxes = tx.get("taxes", 0)
        fees = tx.get("fees", 0)
        instrument["fees"] += fees
        instrument["taxes"] += taxes
        if tx["operation"] == "buy":
            amount = tx["price"] * tx["quantity"]
            total = (instrument["quantity"] + tx["quantity"])
            if total != 0:
                instrument["price"] = (
                    instrument["price"] * instrument["quantity"] +
                    amount
                ) / total
                instrument["average_price_bought"] = (
                    (instrument["average_price_bought"] * instrument["bought"])
                    + amount
                ) / total
            instrument["quantity"] += tx["quantity"]
            instrument["trades"] += 1
            instrument["bought"] += tx["quantity"]
        elif tx["operation"] == "sell":
            amount = tx["price"] * tx["quantity"]
            instrument["quantity"] -= tx["quantity"]
            instrument["trades"] += 1
            instrument["gain"] += ((tx["price"] - instrument["price"]) *
                                   tx["quantity"])
            total = tx["quantity"] + instrument["sold"]
            if total != 0:
                instrument["average_price_sold"] = (
                    (instrument["average_price_sold"] * instrument["sold"])
                    + amount
                ) / total
            instrument["sold"] += tx["quantity"]
        elif tx["operation"] == "dividend":
            amount = tx["price"] * tx["quantity"]
            instrument["dividend"] += amount

    return instruments, currencies
