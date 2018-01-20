CREATE TYPE instrument_type AS ENUM ('stock', 'etf', 'fund');

CREATE TABLE IF NOT EXISTS instruments (
       isin text CHECK (upper(isin) = isin) PRIMARY KEY,
       name text NOT NULL,
       type instrument_type NOT NULL,
       symbol text,
       pea bool,
       pea_pme bool,
       ttf bool,
       exchange_mic text,
       currency text CHECK (upper(currency) = currency) NOT NULL,
       is_alive bool DEFAULT true NOT NULL,
       latest_quote float,
       latest_quote_time timestamp with time zone
);

CREATE TABLE IF NOT EXISTS quotes (
       instrument_isin text REFERENCES instruments(isin) NOT NULL,
       date date NOT NULL,
       open numeric(15, 6),
       close numeric(15, 6),
       high numeric(15, 6),
       low numeric(15, 6),
       volume integer,
       UNIQUE (instrument_isin, date)
);

CREATE TYPE operation_type AS ENUM ('trade', 'dividend', 'tax');

CREATE TABLE IF NOT EXISTS operations (
       portfolio_name text NOT NULL,
       instrument_isin text REFERENCES instruments(isin) NOT NULL,
       type operation_type NOT NULL,
       date date NOT NULL,
       quantity numeric(15, 6) NOT NULL,
       price numeric(15, 6) NOT NULL,
       fees numeric(15, 6) NOT NULL,
       taxes numeric(15, 6) NOT NULL,
       currency text NOT NULL
);

CREATE OR REPLACE VIEW portfolios_history AS
select portfolio_name,
       instrument_isin,
       date,
       position,
       case when total_bought = 0 then null else round(total_spent / total_bought, 2) end as ppu,
       currency,
       ownership_partition
from (
    select *,
           sum(greatest(0, quantity)) over w as total_bought,
           sum(greatest(0, (quantity * price) - fees - taxes)) over w as total_spent
    from (
        select *,
               sum(case when position = 0 then 1 else 0 end) over w as ownership_partition
        from (
                select instrument_isin, date, quantity, price, currency, fees, taxes, portfolio_name,
                sum(quantity) over w as position
                from operations
                where type = 'trade'
                window w as (partition by (portfolio_name, instrument_isin) order by date, quantity desc)
        ) as summed
        window w as (partition by (portfolio_name, instrument_isin) order by date, quantity desc)
    ) as sum_partitioned
    window w as (partition by (portfolio_name, instrument_isin, ownership_partition) order by date)
) as partition_total
order by portfolio_name, instrument_isin, ownership_partition desc, date desc;


CREATE OR REPLACE VIEW portfolios AS
select distinct on (portfolio_name, instrument_isin)
       *
from portfolios_history
order by portfolio_name, instrument_isin, ownership_partition desc, date desc;


CREATE OR REPLACE FUNCTION portfolios_at(_date date)
  RETURNS TABLE (
          portfolio_name text,
          instrument_isin text,
          date date,
          position_ numeric,
          ppu numeric,
          currency text,
          ownership_partition bigint
  ) AS
  $$
  select * from (
    select distinct on (portfolio_name, instrument_isin)
           portfolios_history.*
    from portfolios_history
    where date < _date
  ) as summary
  where summary.position != 0
  $$
  LANGUAGE sql;
