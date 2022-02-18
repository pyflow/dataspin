
import parsy
from parsy import seq
from datetime import date, timedelta
'''
every day at 12h30m
every week day 7 at 8h:30m
every month day 1 
every week day 5 
every week day 1 at 8h:30m
every month day 5 at 8h:50m
'''

class DynamicTimedelta:
    def __init__(self, months=0):
        self.months = months

def _create_parser():
    def _to_int(s):
        if not s:
            return 1
        return int(s)
    
    def _month_timedelta(months=1):
        return DynamicTimedelta(months=months)
    
    def _day_timedelta(day_list):
        if isinstance(day_list, (list, tuple)):
            return map(lambda x: timedelta(days=x-1), day_list)
        return [timedelta(days=day_list-1)]

    space = parsy.string(" ").at_least(1)
    every = parsy.string("every")
    number = parsy.regex('[0-9]+').map(int)

    number_list = (seq(number) + (parsy.string(",") >> number).many()).combine(_day_timedelta)

    month = (parsy.string('months').result('mon') | parsy.string('month').result('mon'))
    week = (parsy.string('weeks').result('w') | parsy.string('week').result('w') )
    day = (parsy.string('days').result('d') | parsy.string('day').result('d') )
    hour = (parsy.string('hours').result('h') | parsy.string('hour').result('h') )
    minute = (parsy.string('minutes').result('m') | parsy.string('minute').result('m'))
    second = (parsy.string('seconds').result('s') | parsy.string('second').result('s'))

    h = parsy.string('h')
    m = parsy.string('m')
    s = parsy.string('s')

    number_month = seq(months =parsy.regex('[0-9]*').map(_to_int), _t = space.optional() >> month).combine_dict(_month_timedelta)
    number_week = seq(weeks =parsy.regex('[0-9]*').map(_to_int), _t = space.optional() >> week).combine_dict(timedelta)
    number_day = seq(days = parsy.regex('[0-9]*').map(_to_int), _t = space.optional() >> day).combine_dict(timedelta)
    number_hour = seq(hours = parsy.regex('[0-9]*').map(_to_int), _t = space.optional() >> hour).combine_dict(timedelta)
    number_minute = seq(minutes = parsy.regex('[0-9]*').map(_to_int), _t = space.optional() >> minute).combine_dict(timedelta)
    number_second = seq(seconds = parsy.regex('[0-9]*').map(_to_int), _t = space.optional() >> second).combine_dict(timedelta)
    
    
    at = parsy.string('at')
    colon = parsy.string(':')
    hour_time = seq(hours = number, _t = h).combine_dict(timedelta)
    minute_time = seq(minutes = number, _t = m).combine_dict(timedelta)
    second_time = seq(seconds = number, _t = s).combine_dict(timedelta)
    day_time = ((hour_time + (colon.optional() >> minute_time) + (colon.optional() >> second_time))
                 | (hour_time + (colon.optional() >> minute_time)) 
                 | (minute_time + (colon.optional() >> second_time))
                 | hour_time | minute_time | second_time )
    at_statement = seq(space >> at, space >> day_time)
    day_statement = seq(parsy.string("day") << space, number_list)
    every_statement = (
                    seq(every << space, number_day | number_hour | number_minute | number_second) 
                    | seq(every << space, (number_week | number_month) << space) + day_statement
                    )
    return every_statement + at_statement | every_statement

scheduler_parser = _create_parser()

def parse_schedule_string(sched_str):
    pass

