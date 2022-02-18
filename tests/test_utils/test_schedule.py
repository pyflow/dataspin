from dataspin.utils.schedule import scheduler_parser
from parsy import ParseError
import pytest
from datetime import timedelta

def test_parse_simple_1():
    assert scheduler_parser.parse('every 1day') == ['every', timedelta(days=1)]
    assert scheduler_parser.parse('every 1 day') == ['every', timedelta(days=1)]
    assert scheduler_parser.parse('every day') == ['every', timedelta(days=1)]
    assert scheduler_parser.parse('every 2 day') == ['every', timedelta(days=2)]
    assert scheduler_parser.parse('every 2day') == ['every', timedelta(seconds=86400*2)]
    with pytest.raises(ParseError):
        scheduler_parser.parse('every1d')

def test_parse_simple_2():
    assert scheduler_parser.parse('every 1day at 12h') == ['every', timedelta(days=1), 'at', timedelta(hours=12)]
    assert scheduler_parser.parse('every 1day at 12h12m') == ['every', timedelta(days=1), 'at', timedelta(hours=12, minutes=12)]
    assert scheduler_parser.parse('every 2 days at 12h2m3s') == ['every', timedelta(days=2), 'at', timedelta(hours=12, minutes=2, seconds=3)]
    assert scheduler_parser.parse('every hour at 2m3s') == ['every', timedelta(hours=1), 'at', timedelta(minutes=2, seconds=3)]

def test_parse_simple_3():
    assert scheduler_parser.parse('every week day 5 at 12h') == ['every', timedelta(weeks=1), 'day', [timedelta(days=4)], 'at', timedelta(hours=12)]