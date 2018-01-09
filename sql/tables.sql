CREATE TABLE exchanges (
       mic text CHECK (upper(mic) = mic) PRIMARY KEY,
       operating_mic text CHECK (upper(operating_mic) = operating_mic),
       name text,
       country text,
       country_code text,
       city text,
       comments text
);
