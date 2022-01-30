
from urllib.parse import urlparse, parse_qsl
import os

from dataspin.utils.util import parse_scheme


def get_provider_class(url):
    parsed = urlparse(url)
    scheme = parsed.scheme
    if scheme in ["s3"]:
        from .aws import SQSStreamProvider, S3StorageProvider
        if scheme == 's3':
            return S3StorageProvider
        elif scheme == 'sqs':
            return SQSStreamProvider
    elif scheme in ["local", "file"]:
        from .local import LocalStreamProvider, LocalStorageProvider
        if scheme == "local":
            return LocalStreamProvider
        if scheme == 'file':
            return LocalStorageProvider
    
    raise Exception(f'No provider for scheme {scheme}')


def get_provider(url, name=None):
    parsed = urlparse(url)
    params = dict(host=parsed.netloc, name=os.path.basename(parsed.path))
    for key, value in parse_qsl(parsed.query):
        if key not in params:
            params[key] = value
    options, platform = parse_scheme(parsed.scheme)
    if platform in ["s3", "sqs"]:
        from .aws import SQSStreamProvider, S3StorageProvider
        if platform == 's3':
            return S3StorageProvider(parsed)
        elif platform == 'sqs':
            print(params)
            return SQSStreamProvider(**params)
    elif platform in ["local", "file"]:
        path = os.path.join(parsed.netloc, parsed.path[1:])
        from .local import LocalStreamProvider, LocalStorageProvider
        if platform == "local":
            return LocalStreamProvider(path, options)
        if platform == 'file':
            return LocalStorageProvider(path, options)
    
    raise Exception(f'No provider for platform {platform}')
