import datetime
import enum

import attr

from greenpoint import utils


class OperationType(enum.Enum):
    TRADE = "trade"
    DIVIDEND = "dividend"
    TAX = "tax"


@attr.s(frozen=True)
class Operation(object):
    instrument_isin = attr.ib(
        validator=attr.validators.optional(
            attr.validators.instance_of(str)),
        converter=attr.converters.optional(str.upper))
    type = attr.ib(validator=attr.validators.instance_of(OperationType))
    date = attr.ib(validator=attr.validators.instance_of(datetime.date))
    quantity = attr.ib(validator=attr.validators.instance_of(float))
    price = attr.ib(validator=attr.validators.instance_of(float))
    fees = attr.ib(validator=attr.validators.instance_of(float))
    taxes = attr.ib(validator=attr.validators.instance_of(float))
    currency = attr.ib(validator=attr.validators.optional(
        attr.validators.instance_of(str)),
                       converter=attr.converters.optional(str.upper))

    @staticmethod
    async def drop_save_all(portfolio_name, operations):
        pool = await utils.get_db()
        async with pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    "DELETE FROM operations "
                    "WHERE portfolio_name = $1",
                    portfolio_name,
                )
                await con.executemany(
                    "INSERT INTO operations "
                    "(portfolio_name, instrument_isin, type, date, "
                    "quantity, price, fees, taxes, currency) "
                    "VALUES "
                    "($1, $2, $3, $4, $5, $6, $7, $8, $9) ",
                    ((portfolio_name,
                      op.instrument_isin,
                      op.type.name.lower(),
                      op.date,
                      op.quantity,
                      op.price,
                      op.fees,
                      op.taxes,
                      op.currency)
                     for op in operations))


async def get_status_for_broker(name, loop=None):
    pool = await utils.get_db(loop=loop)
    return await pool.fetch(
        "select * from portfolios "
        "JOIN instruments ON instrument_isin = isin "
        "where position != 0 and portfolio_name = $1;",
        name)


async def get_status_for_all(loop=None):
    pool = await utils.get_db(loop=loop)
    return await pool.fetch(
        "select *, "
        "position * latest_quote as market_value, "
        "(100 * position * latest_quote) / sum(position * latest_quote) over () as weight, "
        "round(((latest_quote - ppu) * position)::numeric, 2) as potential_gain, "
        "round((100 * (latest_quote - ppu) / ppu)::numeric, 2) as potential_gain_pct "
        "from ("
        "  select instrument_isin, "
        "         sum(position) as position, "
        "         sum(ppu * position) / sum(position) as ppu, "
        "         max(date) as latest_trade "
        "  from portfolios "
        "  where position != 0 "
        "  group by instrument_isin "
        ") as aggregated "
        "join instruments on aggregated.instrument_isin = isin"
    )
