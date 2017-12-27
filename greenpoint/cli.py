import collections
import logging
import operator

import click
import daiquiri

from greenpoint import broker
from greenpoint import config
from greenpoint import storage


LOG = daiquiri.getLogger(__name__)


@click.group()
def main():
    conf = config.get_config()
    daiquiri.setup(level=logging.DEBUG if conf.get("debug") else logging.INFO)


F = None
@main.command(name="import")
@click.argument('broker_name')
def import_(broker_name):
    conf = config.get_config()
    broker_config = conf['brokers'].get(broker_name)
    if broker_config is None:
        raise click.ClickException(
            "Unable to find broker %s in config" % broker_name)
    broker_type = broker.REGISTRY.get(broker_config['type'])
    if broker_type is None:
        raise click.ClickException("Unknown broker type %s" % broker_type)

    LOG.info("Importing transactions for %s", broker_name)
    b = broker_type(broker_config)

    global F
    F = b

    txs = b.list_transactions()
    storage.save_transactions(broker_name, txs)


@main.command()
@click.argument('broker')
def portfolio(broker):
    conf = config.get_config()

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

    instruments = collections.defaultdict(_default_instrument)

    txs = storage.load_transactions(broker)

    for tx in txs:
        key = tuple(sorted(tx["instrument"].items(),
                           key=operator.itemgetter(0)))
        instrument = instruments[key]
        instrument["fees"] += tx.get("fees", 0)
        instrument["taxes"] += tx.get("taxes", 0)
        if tx["operation"] == "buy":
            total = (instrument["quantity"] + tx["quantity"])
            if total != 0:
                instrument["price"] = (
                    instrument["price"] * instrument["quantity"] +
                    tx["price"] * tx["quantity"]
                ) / total
                instrument["average_price_bought"] = (
                    (instrument["average_price_bought"] * instrument["bought"])
                    + tx["price"] * tx["quantity"]
                ) / total
            instrument["quantity"] += tx["quantity"]
            instrument["trades"] += 1
            instrument["bought"] += tx["quantity"]
        elif tx["operation"] == "sell":
            instrument["quantity"] -= tx["quantity"]
            instrument["trades"] += 1
            instrument["gain"] += ((tx["price"] - instrument["price"]) *
                                   tx["quantity"])
            total = tx["quantity"] + instrument["sold"]
            if total != 0:
                instrument["average_price_sold"] = (
                    (instrument["average_price_sold"] * instrument["sold"])
                    + tx["price"] * tx["quantity"]
                ) / total
            instrument["sold"] += tx["quantity"]
        elif tx["operation"] == "dividend":
            instrument["dividend"] += tx["quantity"] * tx["price"]

    import pprint
    for k, v in instruments.items():
        pprint.pprint(k)
        pprint.pprint(v)


if __name__ == '__main__':
    import sys
    sys.exit(main())
