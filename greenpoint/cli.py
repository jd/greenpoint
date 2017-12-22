import logging

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
@click.argument('name')
def import_(name):
    conf = config.get_config()
    broker_config = conf['brokers'].get(name)
    if broker_config is None:
        raise click.ClickException("Unable to find broker %s in config" % name)
    broker_type = broker.REGISTRY.get(broker_config['type'])
    if broker_type is None:
        raise click.ClickException("Unknown broker type %s" % broker_type)

    LOG.info("Importing transactions for %s", name)
    b = broker_type(broker_config)
    
    global F
    F = b

    txs = b.list_transactions()
    storage.save_transactions(name, txs)

# @main.command()
# def fetch():
#     conf = config.get_config()
#     for broker in conf['brokers']:
#         txs = storage.load_transactions(broker)
#         for tx in 


if __name__ == '__main__':
    import sys
    sys.exit(main())
