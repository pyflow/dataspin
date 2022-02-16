
import parsy
from parsy import seq
from datetime import date

def _create_parser():
    space = parsy.string(" ").at_least(1)
    every = parsy.string("every")
    number = parsy.regex('[0-9]+').map(int)
    year = (parsy.string('year') | parsy.string('y'))
    month = (parsy.string('month') | parsy.string('mon') | parsy.string('M'))
    day = (parsy.string('day').result('d') | parsy.string('d'))
    number_day = seq(number.optional(), space.optional() >> day)
    
    week = (parsy.string('week') | parsy.string('w'))
    hour = (parsy.string('hour') | parsy.string('h'))
    minute = (parsy.string('minute') | parsy.string('m'))
    second = (parsy.string('second') | parsy.string('s'))
    at = parsy.string('at')
    return seq(every << space, number_day)

scheduler_parser = _create_parser()

def parse_schedule_string(sched_str):
    pass