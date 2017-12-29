import logging

import click
import daiquiri

from greenpoint import broker
from greenpoint import config
from greenpoint import portfolio as gportfolio
from greenpoint import storage
from greenpoint import utils


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
@click.option('--date')
def portfolio(broker, date=None):
    if date is not None:
        date = utils.parse_date(date)

    txs = storage.load_transactions(broker)

    instruments, currencies = gportfolio.get_portfolio(txs, date)

    import pprint
    for k, v in instruments.items():
        print(dict(k)['name'])
        pprint.pprint(v)
    pprint.pprint(currencies)


if __name__ == '__main__':
    import sys
    sys.exit(main())
