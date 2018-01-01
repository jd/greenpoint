import logging

import click

import daiquiri

import tabulate

from greenpoint import broker
from greenpoint import config
from greenpoint import portfolio as gportfolio
from greenpoint import storage
from greenpoint import utils


LOG = daiquiri.getLogger(__name__)


@click.group()
@click.option('--debug', is_flag=True)
def main(debug=False):
    daiquiri.setup(level=logging.DEBUG if debug else logging.WARNING)


@main.group(name="broker")
def broker_():
    pass

@broker_.command(name="list",
                 help="List configured brokers")
def broker_list():
    conf = config.get_config()
    for b in conf['brokers'].keys():
        click.echo(b)


@broker_.command(name="import",
                 help="Import transactions for brokers. "
                 "Import all brokers by default.")
@click.argument('broker_name', required=False, default=None)
def broker_import(broker_name=None):
    conf = config.get_config()
    if broker_name is not None:
        brokers = [broker_name]
    else:
        brokers = conf['brokers'].keys()
    with click.progressbar(brokers,
                           label='Fetching transactions') as bar:
        for broker_name in bar:
            broker_config = conf['brokers'].get(broker_name)
            if broker_config is None:
                raise click.ClickException(
                    "Unable to find broker %s in config" % broker_name)
            broker_type = broker.REGISTRY.get(broker_config['type'])
            if broker_type is None:
                raise click.ClickException(
                    "Unknown broker type %s" % broker_type)

            LOG.info("Importing transactions for %s", broker_name)
            b = broker_type(broker_config)
            txs = b.list_transactions()
            storage.save_transactions(broker_name, txs)


@main.command()
@click.argument('broker')
@click.option('--date')
def portfolio(broker, date=None):
    if date is not None:
        date = utils.parse_date(date)

    pfl = gportfolio.Portfolio(txs=storage.load_transactions(broker))
    instruments, currencies = pfl.get_portfolio(date)

    print(tabulate.tabulate(
        [
            [
                pi.instrument.name[:29], pi.quantity,
                pi.price, pi.taxes, pi.dividend, pi.fees,
                pi.gain, pi.bought, pi.sold,
                pi.average_price_bought, pi.average_price_sold,
                pi.currency,
                pi.date_first, pi.date_last
            ] for pi in instruments
        ],
        headers=["Instrument", "Qty", "Price", "Taxes", "Div.", "Fees",
                 "Gain", "Bought", "Sold", "Avg P. Bought", "Avg P. Sold",
                 "Curr.",
                 "First", "Last"],
        tablefmt='fancy_grid', floatfmt=".2f"),
    )

    print(tabulate.tabulate([[k, v] for k, v in currencies.items()],
                             headers=["Currency", "Amount"],
                            tablefmt='fancy_grid', floatfmt=".2f"))


if __name__ == '__main__':
    import sys
    sys.exit(main())
