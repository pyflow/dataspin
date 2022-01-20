import datetime
import gzip
import json
import os
from random import randint
import shutil
from typing import List
import uuid


app_ids = ['APPIOXDKXIESP', 'APPOWLSLSDWLD', 'APPSISEKIDESS']

event_names = ['login', 'logout', 'enter_splash', 'leave_splash', 'session']


def generate_json_data(bp_timestamp):
    data_dict = {
        'app_id': app_ids[randint(0, len(app_ids)-1)],
        'event_name': event_names[randint(0, len(event_names)-1)],
        'event_id': str(uuid.uuid4()),
        'bp_timestamp': bp_timestamp
    }
    return json.dumps(data_dict)


def save_json_file(file_dir: str, file_name, data_list: List[str]):
    full_file_name = file_dir+'/'+file_name+'.gz'
    with gzip.open(full_file_name, 'wb') as f:
        f.write('\n'.join(data_list).encode('utf-8'))


def generate_test_data(file_dir='temp', file_numbers=1, data_counts=1000, duplicate_data_count=1, data_format='json', time_range=2, time_unit='day'):
    if time_unit not in ('day', 'hour', 'minute'):
        raise Exception('time_range_unit only support day or hour parameter')
    if file_numbers <= 0:
        raise Exception('file_number should larger than 0')
    if data_counts <= 0:
        raise Exception('data_counts should larger than 0')
    if data_format not in ('json'):
        raise Exception('data_format must be json')
    if time_range < 1:
        raise Exception('data_format must be json')
    file_dir = file_dir.strip('/')
    if os.path.exists(file_dir):
        shutil.rmtree(file_dir)
    os.makedirs(file_dir)
    now = datetime.datetime.utcnow()
    duplicate_data_seq = set()
    for i in range(duplicate_data_count):
        seq = randint(0, data_counts-1)
        while seq in duplicate_data_seq:
            seq = randint(0, data_counts-1)
        duplicate_data_seq.add(seq)
    data_list = []
    for i in range(data_counts):
        if time_unit == 'day':
            start_time = now + datetime.timedelta(days=-time_range)
            target_bp_timestamp = start_time + \
                datetime.timedelta(seconds=randint(0, time_range*24*60*60-1))
        elif time_unit == 'hour':
            start_time = now + datetime.timedelta(hours=-time_range)
            target_bp_timestamp = start_time + \
                datetime.timedelta(seconds=randint(0, time_range*60*60-1))
        elif time_unit == 'minute':
            start_time = now + datetime.timedelta(minutes=-time_range)
            target_bp_timestamp = start_time + \
                datetime.timedelta(seconds=randint(0, time_range*60-1))
        json_data = generate_json_data(
            bp_timestamp=target_bp_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ'))
        data_list.append(json_data)
        if i in duplicate_data_seq:
            data_list.append(json_data)
    if file_numbers > len(data_list):
        for i in range(0, file_numbers):
            save_json_file(file_dir='temp', file_name='%s_temp.log' %
                           i, data_list=data_list[i:i+1] if i < len(data_list) else [])
    else:
        step = int(len(data_list)/file_numbers)+1
        n = 0
        for i in range(0, len(data_list), step):
            save_json_file(file_dir='temp', file_name='%s_temp.log' %
                           n, data_list=data_list[i:i+step])
            n = n + 1


if __name__ == '__main__':
    generate_test_data(file_numbers=1, data_counts=4,
                       duplicate_data_count=0, time_unit='minute')
