
import boto3

class SQSStreamProvider:
    def __init__(self, key_pair, name):
        self.key_pair = key_pair
        self.name = name
        self._sqs_client = boto3.client('sqs')

    def get(self):
        pass

class S3StorageProvider:
    def __init__(self, key_pair):
        self.key_pair = key_pair
    
    def save(self, path, local_path):
        pass