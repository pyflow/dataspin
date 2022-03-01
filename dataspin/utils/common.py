import os
import datetime
import random
from urllib.parse import urlparse
import json
from typing import Any

import pendulum

b32alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567'

b32alphabet_dict = {}

init_value = 0
for key in b32alphabet:
    b32alphabet_dict[key] = init_value
    init_value += 1

DEFAULT_RANDOM_DIGITS = 15
DEFAULT_SERVER_START_TIME = datetime.datetime(2019, 7, 12)
DEFAULT_UUID_LENGTH = 65
DEFAULT_HIGH_LENGTH = 10
DEFAULT_TIMESTAMP_LENGTH = 40

B32_WORD_LENGTH = 5


def parse_url_params(params: str):
    result = {}
    for param in params.split('&'):
        key, value = tuple(param.split('='))
        result[key] = value
    return result


def parse_s3_url(url: str):
    parse_result = urlparse(url)
    if parse_result.path.startswith('/'):
        return parse_result.netloc, parse_result.path[1:]
    else:
        return parse_result.netloc, parse_result.path


def uuid_convert_to_str(uuid, total_digits=DEFAULT_UUID_LENGTH):
    global b32alphabet
    base_chopper = 31
    res = []
    for _ in range(int(total_digits / B32_WORD_LENGTH)):
        res.append(b32alphabet[uuid & base_chopper])
        uuid >>= B32_WORD_LENGTH

    res.reverse()
    return ''.join(res)


def uuid_generator(high='AC', random_digits=DEFAULT_RANDOM_DIGITS):
    global b32alphabet_dict
    table_name = (b32alphabet_dict[high[0]] << 5) | (b32alphabet_dict[high[1]])
    time_stamp = int((datetime.datetime.now() - DEFAULT_SERVER_START_TIME).total_seconds() * 1000)
    rad_digits = random.randint(0, 2 ** random_digits - 1)
    uuid_int = (table_name << (DEFAULT_TIMESTAMP_LENGTH + random_digits)) | (time_stamp << random_digits) | rad_digits
    total_length = DEFAULT_HIGH_LENGTH + DEFAULT_TIMESTAMP_LENGTH + random_digits
    return uuid_convert_to_str(uuid_int, total_length)


def parse_scheme(scheme):
    settings = scheme.split('+')
    if len(settings) > 1:
        options = settings[0: -1]
        platform = settings[-1]
        return options, platform
    else:
        return None, settings[0]


def scantree(path):
    """Recursively yield DirEntry objects for given directory."""
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield from scantree(entry.path)  # see below for Python 2.x
        else:
            yield entry.path

def marshal(d: Any) -> str:
    """
    Marshal to JSON.
    Args:
        d: Any object or value.
    Returns:
        A string containing the JSON-serialized form.
    """
    return json.dumps(d, allow_nan=False, separators=(',', ':'))


def unmarshal(s: str) -> Any:
    """
    Unmarshal a JSON string.
    Args:
        s: A string containing JSON-serialized data.
    Returns:
        The deserialized object or value.
    """
    return json.loads(s)

def format_timestring(date_str) -> str:
    return pendulum.parse(date_str).to_iso8601_string()


def flatten_dict(data: dict, root_key='', delimiter='.'):
    result = {}
    for k, v in data.items():
        k = k.strip()
        key = k if not root_key else root_key + delimiter + k
        if type(v) in [dict] and len(v) != 0:
            result.update(flatten_dict(v, key, delimiter))
        else:
            result[key] = v
    return result


def inflate_dict(flatten_dict, delimiter='.'):
    """
    flatten_dict:{'a.b.c':10,'a.c.d':'value','a.b.e.f':True}
    return :{"a": {"b": {"c": 10, "e": {"f": true}}, "c": {"d": "value"}}}
    """
    result = {}
    for k, v in flatten_dict.items():
        k_list = k.split(delimiter)
        if len(k_list) == 1:
            result[k] = v
        else:
            tmp = result
            for l in k_list[:-1]:
                if l not in tmp:
                    tmp[l] = {}
                tmp = tmp[l]
            tmp[k_list[-1]] = v
    return result
