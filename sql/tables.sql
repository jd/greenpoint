CREATE TABLE exchanges (
       mic text CHECK (upper(mic) = mic) PRIMARY KEY,
       operating_mic text CHECK (upper(operating_mic) = operating_mic) NOT NULL,
       name text NOT NULL,
       country text,
       country_code text,
       city text,
       comments text
);

CREATE TYPE instrument_type AS ENUM ('stock', 'etf', 'fund');

CREATE TABLE instruments (
       isin text CHECK (upper(isin) = isin) PRIMARY KEY,
       name text NOT NULL,
       type instrument_type NOT NULL,
       symbol text,
       pea bool,
       pea_pme bool,
       ttf bool,
       exchange_mic text REFERENCES exchanges(mic),
       currency text CHECK (upper(currency) = currency) NOT NULL,
       is_alive bool DEFAULT true NOT NULL,
       latest_quote float,
       latest_quote_time timestamp with time zone
);

CREATE TABLE quotes (
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

CREATE TABLE operations (
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
