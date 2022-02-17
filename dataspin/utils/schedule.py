
import parsy
from parsy import seq
from datetime import date

def _create_parser():
    def _to_int(s):
        if not s:
            return 1
        return int(s)
    space = parsy.string(" ").at_least(1)
    every = parsy.string("every")
    number = parsy.regex('[0-9]+').map(int)
    year = (parsy.string('year').result('y') | parsy.string('y'))
    month = (parsy.string('month').result('mon') | parsy.string('mon') | parsy.string('M').result('mon'))
    day = (parsy.string('day').result('d') | parsy.string('d'))
    week = (parsy.string('week').result('w') | parsy.string('w'))
    hour = (parsy.string('hour').result('h') | parsy.string('h'))
    minute = (parsy.string('minute').result('m') | parsy.string('m'))
    second = (parsy.string('second').result('s') | parsy.string('s'))

    number_month = seq(parsy.regex('[0-9]*').map(_to_int), space.optional() >> month) 
    number_week = seq(parsy.regex('[0-9]*').map(_to_int), space.optional() >> week)
    number_day = seq(parsy.regex('[0-9]*').map(_to_int), space.optional() >> day)
    number_hour = seq(parsy.regex('[0-9]*').map(_to_int), space.optional() >> hour)
    number_second = seq(parsy.regex('[0-9]*').map(_to_int), space.optional() >> second)
    
    
    at = parsy.string('at')
    day_time = (seq(number, hour, number, minute, number, second) | seq(number, hour, number, minute)
                | seq(number, minute, number, second) | seq(number, hour) | seq(number, minute) | seq(number, second))
    at_statement = seq(space >> at, space >> day_time)
    every_statement = seq(every << space, number_month | number_week | number_day | number_hour | number_second)
    return every_statement + at_statement | every_statement

scheduler_parser = _create_parser()

def parse_schedule_string(sched_str):
    pass