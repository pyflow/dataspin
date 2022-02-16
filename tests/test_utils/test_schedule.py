from dataspin.utils.schedule import scheduler_parser
from parsy import ParseError
import pytest

def test_parse_simple_1():
    assert scheduler_parser.parse('every 1d') == ['every', [1, 'd']]
    assert scheduler_parser.parse('every 1day') == ['every', [1, 'd']]
    assert scheduler_parser.parse('every 1 day') == ['every', [1, 'd']]
    with pytest.raises(ParseError):
        scheduler_parser.parse('every1d')