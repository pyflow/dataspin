
import boto3

class SQSStreamProvider:
    def __init__(self, name=None, access_key=None, secret_key=None, **kwargs):
        self.access_key = access_key
        self.secret_key = secret_key
        self.name = name
        self._sqs_client = boto3.client('sqs')

    def get(self):
        pass

class S3StorageProvider:
    def __init__(self, path=None, access_key=None, secret_key=None, **kwargs):
        self.access_key = access_key
        self.secret_key = secret_key
    
    def save(self, path, local_path):
        pass