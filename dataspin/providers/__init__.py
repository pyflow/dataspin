
from urllib.parse import urlparse

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
        from .local import LocalStreamProvider
        if scheme == "local":
            return LocalStreamProvider
    
    raise Exception(f'No provider for scheme {scheme}')

def get_provider(url, name=None):
    parsed = urlparse(url)
    scheme = parsed.scheme
    if scheme in ["s3"]:
        from .aws import SQSStreamProvider, S3StorageProvider
        if scheme == 's3':
            return S3StorageProvider(parsed)
        elif scheme == 'sqs':
            return SQSStreamProvider(parsed)
    elif scheme in ["local", "file"]:
        from .local import LocalStreamProvider
        if scheme == "local":
            return LocalStreamProvider(parsed)
    
    raise Exception(f'No provider for scheme {scheme}')