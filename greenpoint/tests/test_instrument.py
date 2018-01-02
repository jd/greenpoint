import datetime

from greenpoint import instrument


def test_get_by_name():
    ex = instrument.get_exchange_by_name("Euronext Paris")
    assert ex.mic == "XPAR"
    ex = instrument.get_exchange_by_name("Euronext Bruxelles")
    assert ex.mic == "XBRU"
    ex = instrument.get_exchange_by_name("NEW YORK STOCK EXCHANGE")
    assert ex.mic == "XNYS"


def test_quotes_from_lesechos():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.fetch_quotes_from_lesechos()
    assert instrument.Quote(date=datetime.date(2017, 12, 20),
                            open=16.73,
                            close=17.69,
                            high=17.69,
                            low=16.25,
                            volume=252461) in quotes
    inst = instrument.Instrument(
        isin="FR0011665281",
        type=instrument.InstrumentType.STOCK,
        name="Invalid",
        symbol="FGAXX",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.fetch_quotes_from_lesechos()
    assert list(quotes) == []


def test_quotes_from_boursorama():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.fetch_quotes_from_boursorama()
    assert instrument.Quote(date=datetime.date(2017, 12, 20),
                            open=16.73,
                            close=17.69,
                            high=17.69,
                            low=16.25,
                            volume=179707) in quotes
    inst = instrument.Instrument(
        isin="FR0011665281",
        type=instrument.InstrumentType.STOCK,
        name="Invalid",
        symbol="FGAXX",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.fetch_quotes_from_boursorama()
    assert list(quotes) == []


def test_quotes_from_google():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.fetch_quotes_from_google()
    assert instrument.Quote(date=datetime.date(2017, 12, 20),
                            open=16.73,
                            close=17.69,
                            high=17.69,
                            low=16.25,
                            volume=179707) in quotes
    inst = instrument.Instrument(
        isin="FR0011665281",
        type=instrument.InstrumentType.STOCK,
        name="Invalid",
        symbol="FGAXX",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.fetch_quotes_from_google()
    assert list(quotes) == []


def test_quotes():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.fetch_quotes().values()
    assert instrument.Quote(date=datetime.date(2017, 12, 20),
                            open=16.73,
                            close=17.69,
                            high=17.69,
                            low=16.25,
                            volume=179707) in quotes
    inst = instrument.Instrument(
        isin="FR0011665281",
        type=instrument.InstrumentType.STOCK,
        name="Invalid",
        symbol="FGAXX",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.fetch_quotes().values()
    assert list(quotes) == []


def test_quotes_property():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    assert instrument.Quote(date=datetime.date(2017, 12, 20),
                            open=16.73,
                            close=17.69,
                            high=17.69,
                            low=16.25,
                            volume=252461) in inst.quotes.values()
    inst = instrument.Instrument(
        isin="FR0011665281",
        type=instrument.InstrumentType.STOCK,
        name="Invalid",
        symbol="FGAXX",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.quotes.values()
    assert list(quotes) == []


def test_live_quote_from_yahoo():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quote = inst.fetch_live_quote_from_yahoo()
    assert isinstance(quote, instrument.Quote)
    # Close is current price
    assert quote.low <= quote.close <=  quote.high
    inst = instrument.Instrument(
        isin="FR0011665281",
        type=instrument.InstrumentType.STOCK,
        name="Invalid",
        symbol="FGAXX",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quote = inst.fetch_live_quote_from_yahoo()
    assert quote == None


def test_quote_property():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quote = inst.quote
    assert isinstance(quote, instrument.Quote)
    # Close is current price
    assert quote.low <= quote.close <=  quote.high
    inst = instrument.Instrument(
        isin="FR0011665281",
        type=instrument.InstrumentType.STOCK,
        name="Invalid",
        symbol="FGAXX",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quote = inst.quote
    assert quote == None
