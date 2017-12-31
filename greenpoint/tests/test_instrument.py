import datetime

from greenpoint import instrument


def test_get_by_name():
    ex = instrument.get_exchange_by_name("Euronext Paris")
    assert ex.mic == "XPAR"
    ex = instrument.get_exchange_by_name("Euronext Bruxelles")
    assert ex.mic == "XBRU"


def test_quotes_from_lesechos():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.get_quotes_from_lesechos()
    assert instrument.Quote(date=datetime.date(2017, 12, 20),
                            open=16.73,
                            close=17.69,
                            high=17.69,
                            low=16.25,
                            volume=252461) in quotes


def test_quotes_from_boursorama():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.get_quotes_from_boursorama()
    assert instrument.Quote(date=datetime.date(2017, 12, 20),
                            open=16.73,
                            close=17.69,
                            high=17.69,
                            low=16.25,
                            volume=179707) in quotes


def test_quotes_from_google():
    inst = instrument.Instrument(
        isin="FR0011665280",
        type=instrument.InstrumentType.STOCK,
        name="Figeac Aero",
        symbol="FGA",
        exchange=instrument.get_exchange_by_mic("XPAR"),
        pea=None, pea_pme=None, ttf=None)
    quotes = inst.get_quotes_from_google()
    assert instrument.Quote(date=datetime.date(2017, 12, 20),
                            open=16.73,
                            close=17.69,
                            high=17.69,
                            low=16.25,
                            volume=179707) in quotes
