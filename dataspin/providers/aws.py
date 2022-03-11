import gzip
from io import BytesIO
import json
import tempfile
import traceback
import boto3

from basepy.log import logger
from dataspin.utils.common import parse_url_params, parse_s3_url

from dataspin.message.message import StreamMessage


class SQSStreamProvider:
    def __init__(self, name=None, access_key=None, secret_key=None, region=None, **kwargs):
        sqs = boto3.resource('sqs',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key,
                             region_name = region)
        self._queue = sqs.get_queue_by_name(QueueName=name)
        self._pendding_message = []

    def get(self,block=True, timeout=None):
        message_list = self._queue.receive_messages(
            MaxNumberOfMessages=1)
        if message_list:
            message = message_list[0]
            self._pendding_message.append(message)
            body = json.loads(message.body)
            if body.get('data_format') == 'dataspin':
                return self._parse_dataspin((body))
            return self._parse_s3_message(body)
        return None

    def _parse_s3_message(self,body):
        try:
            records = body['Records']
        except KeyError:  # 测试数据可能会没有Records字段
            logger.error('test data do not contain Records field')
        else:
            record = records[0]
            try:
                s3 = record['s3']
                bucket = s3['bucket']['name']
                key = s3['object']['key']
                return StreamMessage('s3',bucket,key,None)
            except Exception as e:
                logger.error('parse sqs record error, error: {},record: {}\ntraceback:{}'.
                                format(e, record, traceback.format_exc()))
                raise e
    
    def _parse_dataspin(self,body):
        record = body['record']
        tags = body.get('tags')
        return StreamMessage(record['storage_type'],record['bucket'],record['key'],tags)

    def send_message(self, message:StreamMessage):
        body = {'data_format': 'dataspin',
                'record': {
                    'bucket': message.bucket,
                    'key': message.key,
                    'storage_type': message.storage_type},
                'tags': message.tags}
        logger.debug('send sqs message body',body=body)
        self._queue.send_message(MessageBody=json.dumps(body))

    def task_done(self, file_path):
        message = self._pendding_message.pop()
        message.delete()


class S3StorageProvider:
    def __init__(self, path=None, access_key=None, secret_key=None,region=None, **kwargs):
        self._s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name = region
        )
        self._path = path
        self._bucket, self._prefix = path.split('/', 1)

    @property
    def storage_type(self):
        return 's3'
        
    @property
    def path(self):
        return self._path

    def get(self):
        paginator = self._s3_client.get_paginator("list_objects_v2")
        for res in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for item in res.get("Contents", []):
                yield self._bucket+'/'+item['Key']

    def fetch_file(self,file_path):
        bucket ,key = file_path.split('/',1)
        with tempfile.TemporaryFile('w+b') as fp:
            self._s3_client.download_fileobj(bucket, key, fp)
            fp.seek(0)
            yield fp

    def save(self, key, local_file):
        key = self._prefix + '/' + key
        self._s3_client.upload_file(local_file, self._bucket, key)
        return self._bucket + '/' + key

    def save_data(self, key, lines):
        key = self._prefix + '/' + key
        data = BytesIO(gzip.compress('\n'.join(lines).encode('utf-8')))
        self.s3_client.upload_fileobj(data, self._bucket, key)
        return self._bucket + '/' + key