
import datetime
import random
from urllib.parse import urlparse


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