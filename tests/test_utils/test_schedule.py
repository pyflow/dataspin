from dataspin.utils.schedule import scheduler_parser
from parsy import ParseError
import pytest

def test_parse_simple_1():
    assert scheduler_parser.parse('every 1d') == ['every', [1, 'd']]
    assert scheduler_parser.parse('every 1day') == ['every', [1, 'd']]
    assert scheduler_parser.parse('every 1 day') == ['every', [1, 'd']]
    assert scheduler_parser.parse('every day') == ['every', [1, 'd']]
    assert scheduler_parser.parse('every 2 day') == ['every', [2, 'd']]
    assert scheduler_parser.parse('every 2day') == ['every', [2, 'd']]
    with pytest.raises(ParseError):
        scheduler_parser.parse('every1d')

def test_parse_simple_2():
    assert scheduler_parser.parse('every 1d at 12h') == ['every', [1, 'd'], 'at', [12, 'h']]
    assert scheduler_parser.parse('every 1d at 12h12m') == ['every', [1, 'd'], 'at', [12, 'h', 12, 'm']]
    assert scheduler_parser.parse('every 2d at 12h2m3s') == ['every', [2, 'd'], 'at', [12, 'h', 2, 'm', 3, 's']]
    assert scheduler_parser.parse('every hour at 2m3s') == ['every', [1, 'h'], 'at', [2, 'm', 3, 's']]