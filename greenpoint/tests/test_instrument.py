from greenpoint import instrument


def test_get_by_name():
    ex = instrument.get_exchange_by_name("Euronext Paris")
    assert ex.mic == "XPAR"
    ex = instrument.get_exchange_by_name("Euronext Bruxelles")
    assert ex.mic == "XBRU"
