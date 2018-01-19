import asyncio
import logging

import attr

import click

import colorama

import daiquiri

import tabulate

import termcolor

from greenpoint import broker
from greenpoint import instrument
from greenpoint import portfolio as gportfolio
from greenpoint import utils
from greenpoint import web as gweb

LOG = daiquiri.getLogger(__name__)


@click.group()
@click.option('--debug', is_flag=True)
def main(debug=False):
    colorama.init()
    daiquiri.setup(level=logging.DEBUG if debug else logging.WARNING)


@main.command(name="web")
def web():
    return gweb.app.run(debug=True)


@main.group(name="broker")
def broker_():
    pass


@broker_.command(name="list",
                 help="List configured brokers")
def broker_list():
    conf = utils.get_config()
    for b in conf['brokers'].keys():
        click.echo(b)


@broker_.command(name="import",
                 help="Import transactions for brokers. "
                 "Import all brokers by default.")
@click.argument('broker_name', required=False, default=None)
def broker_import(broker_name=None):
    conf = utils.get_config()
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


def color_value(v, suffix=""):
    if v is None:
        return
    if v < 0:
        return termcolor.colored("%.2f%s" % (v, suffix), "red")
    if v > 0:
        return termcolor.colored("%.2f%s" % (v, suffix), "green")
    return v


@main.group(name="portfolio")
def portfolio_group():
    pass


@portfolio_group.command(name="show")
@click.argument('broker_name', required=False, default=None)
def portfolio_show(broker_name=None):
    loop = asyncio.get_event_loop()
    if broker_name:
        f = gportfolio.get_status_for_broker(broker_name)
    else:
        f = gportfolio.get_status_for_all()

    status = loop.run_until_complete(f)

    headers = {
        "instrument_isin": "ISIN",
        "name": "Name",
        "date": "Latest trade",
        "position": "Position",
        "ppu": "PPU",
        "latest_quote": "Quote",
        "latest_quote_time": "@",
        "market_value": "Mkt val",
        "potential_gain": "Gain",
        "potential_gain_pct": "Gain %",
    }

    if status:
        real_headers = []
        lines = []
        for line in status:
            values = []
            for k, v in line.items():
                if k in headers.keys():
                    if k == 'name':
                        v = v[:30]
                    elif k == 'potential_gain':
                        v = color_value(v)
                    elif k == 'potential_gain_pct':
                        v = color_value(v, "%")
                    values.append(v)
                    if len(real_headers) != len(values):
                        real_headers.append(headers[k])
            lines.append(values)

        print(tabulate.tabulate(
            lines,
            headers=real_headers,
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
