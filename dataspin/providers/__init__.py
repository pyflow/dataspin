
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
        from .local import LocalStreamProvider, LocalStorageProvider
        if scheme == "local":
            return LocalStreamProvider
        if scheme == 'file':
            return LocalStorageProvider
    
    raise Exception(f'No provider for scheme {scheme}')

def get_provider(url, name=None):
    parsed = urlparse(url)
    scheme = parsed.scheme
    if scheme in ["s3", "sqs"]:
        from .aws import SQSStreamProvider, S3StorageProvider
        if scheme == 's3':
            return S3StorageProvider(parsed)
        elif scheme == 'sqs':
            return SQSStreamProvider(parsed)
    elif scheme in ["local", "file"]:
        from .local import LocalStreamProvider, LocalStorageProvider
        if scheme == "local":
            return LocalStreamProvider(parsed)
        if scheme == 'file':
            return LocalStorageProvider(parsed)
    
    raise Exception(f'No provider for scheme {scheme}')


if __name__ == '__main__':
    a = urlparse('sqs://bytepower-data-parquet-schedule-modelspin?region=cn-northwest-1&aws_access_key_id=xxx&aws_secret_access_key=xxx')
    print(a)