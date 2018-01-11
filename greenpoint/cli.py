import asyncio
import logging

import attr

import click

import colorama

import daiquiri

import tabulate

import termcolor

from greenpoint import broker
from greenpoint import config
from greenpoint import instrument
from greenpoint import portfolio as gportfolio
from greenpoint import utils


LOG = daiquiri.getLogger(__name__)


@click.group()
@click.option('--debug', is_flag=True)
def main(debug=False):
    colorama.init()
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
    loop = asyncio.get_event_loop()
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
            b = broker_type(broker_name, broker_config)

            async def _import_operations():
                await gportfolio.Operation.drop_save_all(
                    broker_name,
                    await b.list_transactions())

            loop.run_until_complete(_import_operations())


def color_value(v):
    if v < 0:
        return termcolor.colored("%.2f" % v, "red")
    if v > 0:
        return termcolor.colored("%.2f" % v, "green")
    return v


@main.group(name="portfolio")
def portfolio_group():
    pass


@portfolio_group.command(name="txs")
@click.argument('broker')
@click.option('--date')
@click.option('--all', 'include_all', is_flag=True)
def portfolio_txs(broker, date=None, include_all=False):
    if date is not None:
        date = utils.parse_date(date)

    pfl = gportfolio.Portfolio(txs=storage.load_transactions(broker))
    instruments, currencies = pfl.get_portfolio(date)

    print(tabulate.tabulate(
        [
            [
                termcolor.colored(pi.instrument.name[:30], attrs=['bold']),
                pi.quantity,
                pi.price, pi.taxes, pi.dividend, pi.fees,
                color_value(pi.gain),
                pi.bought, pi.sold,
                pi.average_price_bought, pi.average_price_sold,
                pi.currency,
                pi.date_first, pi.date_last
            ] for pi in sorted(instruments, key=lambda pi: pi.instrument.name)
            # Use != so we show what might be negative and is a "bug"
            if include_all or pi.quantity != 0
        ],
        headers=["Instrument", "Qty", "Price", "Taxes", "Div.", "Fees",
                 "Gain", "Bought", "Sold", "Avg P. Bought", "Avg P. Sold",
                 "$",
                 "First", "Last"],
        tablefmt='fancy_grid', floatfmt=".2f"),
    )

    print(tabulate.tabulate(
        [
            [color_value(sum(pi.gain for pi in instruments))],
        ],
        headers=(termcolor.colored("Gain"),),
        tablefmt='fancy_grid', floatfmt=".2f")
    )

    print(tabulate.tabulate([[termcolor.colored(k, attrs=['bold']),
                              color_value(v)]
                             for k, v in currencies.items()],
                            headers=["Currency", "Amount"],
                            tablefmt='fancy_grid', floatfmt=".2f"))


@portfolio_group.command(name="show")
@click.argument('broker')
def portfolio_show(broker):
    pfl = gportfolio.Portfolio(txs=storage.load_transactions(broker))
    instruments, currencies = pfl.get_portfolio()

    lines = []
    for pi in sorted(instruments, key=lambda pi: pi.instrument.name):
        # Use == so we show what might be negative and is a "bug"
        if pi.quantity == 0:
            continue

        potential_gain = pi.potential_gain()
        potential_gain_since_last = pi.potential_gain(-1)

        lines.append([
            termcolor.colored(pi.instrument.name[:30], attrs=['bold']),
            pi.quantity,
            # FIXME(jd) Compute price with fees and taxes
            pi.price,
            pi.instrument.quote.close if pi.instrument.quote else "?",
            color_value(potential_gain)
            if potential_gain is not None else "?",
            color_value(potential_gain_since_last)
            if potential_gain_since_last is not None else "?",
            pi.currency,
            pi.instrument.quote.date if pi.instrument.quote else "?",
        ])

    print(tabulate.tabulate(
        lines,
        headers=["Instrument", "Qty", "B. Price", "M. Price",
                 "Gain", "Last Daily Gain",
                 "$", "L. Trade"],
        tablefmt='fancy_grid', floatfmt=".2f",
    ))


@main.group(name="instrument")
def instrument_group():
    pass


@instrument_group.command(name="list")
def instrument_list():
    loop = asyncio.get_event_loop()
    instruments = []

    for inst in loop.run_until_complete(
            instrument.Instrument.list_instruments()):
        headers = list(map(str.capitalize, attr.asdict(inst).keys()))
        headers.remove("Is_alive")
        values = []
        for k, v in attr.asdict(inst).items():
            if k == "exchange":
                if v:
                    values.append(v['name'])
                else:
                    values.append("?")
            elif k == "type":
                values.append(v.name)
            elif k == "quotes":
                values.append(len(v))
            elif k == "name":
                values.append(v[:28])
            elif k in ("pea", "pea_pme", "ttf"):
                if v:
                    values.append("X")
                else:
                    values.append("")
            elif k == "is_alive":
                continue
            else:
                values.append(v)
        instruments.append(values)

    click.echo(tabulate.tabulate(
        instruments,
        headers=headers,
        tablefmt='fancy_grid', floatfmt=".2f",
    ))


async def _update_instrument(name):
    if name is None:
        instruments = await instrument.Instrument.list_instruments()
        click.echo("Updating %d instruments" % len(instruments))
    else:
        try:
            instruments = [await instrument.Instrument.load(name=name)]
        except TypeError:
            raise click.ClickException("Unknown instrument %s" % name)
        click.echo("Updating %s" % instruments[0])

    futures = []
    with click.progressbar(instruments,
                           label='Scheduling quote updates') as insts:
        for inst in insts:
            futures.append(asyncio.ensure_future(inst.refresh_quotes()))
            futures.append(asyncio.ensure_future(inst.refresh_live_quote()))
    with click.progressbar(futures,
                           label='Waiting for quote updates') as futs:
        for fut in futs:
            await fut


@instrument_group.command(name="update")
@click.argument('name', required=False)
def instrument_update(name=None):
    loop = asyncio.get_event_loop()

    loop.run_until_complete(_update_instrument(name))
    loop.close()


if __name__ == '__main__':
    import sys
    sys.exit(main())
