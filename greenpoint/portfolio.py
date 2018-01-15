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


async def get_status():
    pool = await utils.get_db()
    return await pool.fetch(
        """
select *,
       position * latest_quote as market_value,
       round(((latest_quote - ppu) * position)::numeric, 2) as potential_gain,
       round((100 * (latest_quote - ppu) / ppu)::numeric, 2) as potential_gain_pct
from (
    select distinct on (instrument_isin) instrument_isin,
           instruments.name,
           date as latest_trade,
           position,
           case when total_bought = 0 then null else round(total_spent / total_bought, 2) end as ppu,
           partition_total.currency as op_currency,
           latest_quote,
           instruments.currency as instrument_currency,
           latest_quote_time
    from (
        select *,
               sum(greatest(0, quantity)) over w as total_bought,
               sum(greatest(0, (quantity * price) - fees - taxes)) over w as total_spent
        from (
            select *,
                   sum(case when position = 0 then 1 else 0 end) over w as ownership_partition
            from (
                    select instrument_isin, date, quantity, price, currency, fees, taxes,
                    sum(quantity) over w as position
                    from operations
                    where type = 'trade'
                    window w as (partition by (instrument_isin) order by date, quantity desc)
            ) as summed
            window w as (partition by (instrument_isin) order by date, quantity desc)
        ) as sum_partitioned
        window w as (partition by (instrument_isin, ownership_partition) order by date)
    ) as partition_total, instruments
    where instrument_isin = instruments.isin
    order by instrument_isin, ownership_partition desc, date desc
) as finalfiltering
where position != 0
order by market_value
""")
