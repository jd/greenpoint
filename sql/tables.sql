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
       currency text CHECK (upper(currency) = currency) NOT NULL
);
