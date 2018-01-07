import datetime

import pytest

from greenpoint import instrument


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
    inst.refresh_quotes()
    quotes = inst.quotes.values()
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
    inst.refresh_quotes()
    quotes = inst.quotes.values()
    assert list(quotes) == []


def test_save_load():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    inst.refresh_quotes()
    inst.save()
    inst2 = instrument.Instrument.load(isin="FR0011665280")
    assert inst == inst2


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
    assert quote.low <= quote.close <= quote.high
    inst = instrument.Instrument(
        isin="FR0011665281",
        type=instrument.InstrumentType.STOCK,
        name="Invalid",
        symbol="FGAXX",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quote = inst.fetch_live_quote_from_yahoo()
    assert quote is None


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
    assert quote.low <= quote.close <= quote.high

    inst = instrument.Instrument(
        isin="FR0011665281",
        type=instrument.InstrumentType.STOCK,
        name="Invalid",
        symbol="FGAXX",
        currency="EUR",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quote = inst.quote
    assert quote is None

    # Check case of currency
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        currency="USD",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    with pytest.raises(ValueError) as e:
        inst.quote
        assert "Quote returned by Yahoo is in currency" in e


def test_quotes_list():
    q1 = instrument.Quote(date=datetime.date(2016, 12, 12),
                          low=None, high=None, open=None, close=None,
                          volume=None)
    q2 = instrument.Quote(date=datetime.date(2016, 12, 18),
                          low=None, high=None, open=None, close=None,
                          volume=None)
    q3 = instrument.Quote(date=datetime.date(2016, 12, 19),
                          low=None, high=None, open=None, close=None,
                          volume=None)
    q4 = instrument.Quote(date=datetime.date(2016, 12, 21),
                          low=None, high=None, open=None, close=None,
                          volume=None)
    ql = instrument.QuoteList([q1, q2, q4, q3])
    assert ql[0] == q1
    assert ql[1] == q2
    assert ql[2] == q3
    assert ql[3] == q4
    assert ql[q3.date] == q3
    assert ql[q2.date] == q2
    assert ql[:q3.date] == [q1, q2, q3]
    assert ql[q1.date:q3.date] == [q1, q2, q3]
    assert ql[q2.date:] == [q2, q3, q4]
    assert ql[datetime.date(2017, 1, 1):] == []
    assert ql[:datetime.date(2015, 1, 1)] == []
    assert ql[:datetime.date(2016, 12, 13)] == [q1]
    assert ql[datetime.date(2016, 12, 13):] == [q2, q3, q4]
