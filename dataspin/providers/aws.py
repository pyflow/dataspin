import boto3
import os

from basepy.log import logger
from dataspin.utils.common import parse_url_params, parse_s3_url


class SQSStreamProvider:
    def __init__(self, name=None, access_key=None, secret_key=None, **kwargs):
        self.access_key = access_key
        self.secret_key = secret_key
        self.name = name
        self._sqs_client = boto3.client('sqs')

    MESSAGE_BATCH_SIZE = 1

    def __init__(self, key_pair):
        self._key_pair = key_pair
        self._sqs_client = None
        self._mq = None
        self._s3_client = None

    def _init_sqs(self):
        queue_name = self._key_pair.netloc
        params_dict = parse_url_params(self._key_pair.query)
        self._sqs_client = boto3.resource('sqs',
                                          region_name=params_dict.get('region'),
                                          aws_access_key_id=params_dict.get('aws_access_key_id'),
                                          aws_secret_access_key=params_dict.get('aws_secret_access_key'))
        self._mq = self._sqs_client.get_queue_by_name(QueueName=queue_name)
        self._s3_client = boto3.client('s3',
                                       region_name=params_dict.get('region'),
                                       aws_access_key_id=params_dict.get('aws_access_key_id'),
                                       aws_secret_access_key=params_dict.get('aws_secret_access_key'))

    def get(self, storage_path):
        if self._sqs_client is None or self._mq is None:
            self._init_sqs()
        try:
            messages = self._mq.receive_messages(MaxNumberOfMessages=self.MESSAGE_BATCH_SIZE)
            for message in messages:
                obj_key = message.body
                if obj_key:
                    bucket, obj_path = parse_s3_url(obj_key)
                    obj_name = obj_path.split('/')[-1]
                    os.makedirs(os.path.dirname(storage_path + obj_name), exist_ok=True)
                    with open(storage_path + obj_name, 'w+b') as data:
                        self._s3_client.download_fileobj(bucket, obj_path, data)
                # if prefix:
                #     bucket, prefix_path = parse_s3_url(prefix)
                #     paginator = self._s3_client.get_paginator("list_objects_v2")
                #     for res in paginator.paginate(Bucket=bucket, Prefix=prefix_path):
                #         for item in res.get("Contents", []):
                #             self.get(storage_path, obj_key='s3://{bucket}/{key}'.format(bucket=bucket,
                #                                                                         key=item['Key']))
        except Exception:
            logger.error('Exception')



class S3StorageProvider:
    def __init__(self, path=None, access_key=None, secret_key=None, **kwargs):
        self.access_key = access_key
        self.secret_key = secret_key
        self._s3_client = None
    
    def save(self, path, local_path):
        pass


    def _init_s3(self):
        params_dict = parse_url_params(self._key_pair.query)
        self._s3_client = boto3.client('s3',
                                       region_name=params_dict.get('region'),
                                       aws_access_key_id=params_dict.get('aws_access_key_id'),
                                       aws_secret_access_key=params_dict.get('aws_secret_access_key'))
        self._bucket, self._prefix = self._key_pair.netloc, self._key_pair.path[1:]

    def get(self, storage_path, obj_key=None, prefix=None):
        if self._s3_client is None:
            self._init_s3()
        if obj_key:
            bucket, obj_path = parse_s3_url(obj_key)
            obj_name = obj_path.split('/')[-1]
            os.makedirs(os.path.dirname(storage_path + obj_name), exist_ok=True)
            with open(storage_path + obj_name, 'w+b') as data:
                self._s3_client.download_fileobj(bucket, obj_path, data)
        # if prefix:
        #     bucket, prefix_path = parse_s3_url(prefix)
        #     paginator = self._s3_client.get_paginator("list_objects_v2")
        #     for res in paginator.paginate(Bucket=bucket, Prefix=prefix_path):
        #         for item in res.get("Contents", []):
        #             self.get(storage_path, obj_key='s3://{bucket}/{key}'.format(bucket=bucket,
        #                                                                         key=item['Key']))

    # def save(self, source_path, destination_path=None):
    #     with open(source_path, 'rb') as data:
    #         self._s3_client.upload_fileobj(data, self._bucket, self._prefix)

