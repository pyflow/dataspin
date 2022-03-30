import gzip
from io import BytesIO
import json
import tempfile
import traceback
import pulsar
from _pulsar import ConsumerType
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from basepy.log import logger


class TDMQStreamProvider:

    def __init__(self, host=None, token=None, topic=None, subscription_name=None,**kwargs):
        self._client = pulsar.Client(
            service_url='http://'+host, authentication=pulsar.AuthenticationToken(token))
        self._consumer = self._client.subscribe(topic=topic,
                                                subscription_name=subscription_name,
                                                consumer_type=ConsumerType.Shared)
        self._producer = self._client.create_producer(topic=topic)
        self._pendding_message = []

    def get(self, block=True, timeout=None):
        message = self._consumer.receive(timeout_millis=timeout)
        if message:
            self._pendding_message.append(message)
            body = json.loads(message.data())
            if body.get('data_format') == 'dataspin':
                return body
            else:
                return self._transform_raw_cos(body)
        return None

    def _transform_raw_cos(self, body):
        try:
            bucket = body.get('cos').get('cosBucket').get('name')
            key = body.get('cos').get('cosObject').get('key')
            return dict(file_url = f'cos://{bucket}{key}')
        except Exception as e:
            logger.error('read tdmq message error = %s' % repr(e))
            raise e

    def send_message(self, message: dict):
        logger.debug('send tdmq message body', body=message)
        self._producer.send(content=json.dumps(message).encode('utf-8'))

    def task_done(self, file_path):
        message = self._pendding_message.pop()
        self._consumer.acknowledge(message)


class COSStorageProvider:
    def __init__(self, path=None, access_key=None, secret_key=None, region=None, **kwargs):
        config = CosConfig(Region=region, SecretId=access_key,
                           SecretKey=secret_key, Token=None, Scheme='https')
        self._client = CosS3Client(config)
        self._path = path
        self._bucket, self._prefix = path.split('/', 1)

    @property
    def path(self):
        return self._path

    @property
    def storage_type(self):
        return 'cos'

    def get(self,prefix=None):
        marker = ''
        while True:
            response = self._client.list_objects(
                Bucket=self._bucket, Prefix=prefix if prefix else self._prefix, Marker=marker)
            contents = response.get('Contents')
            if not contents:
                break
            for content in contents:
                yield self._bucket + '/' + content['Key']
            if response['IsTruncated'] == 'false':
                break
            marker = response['NextMarker']

    def fetch_file(self, file_path):
        bucket, key = file_path.split('/', 1)
        with tempfile.TemporaryFile('w+b') as fp:
            response = self._client.get_object(
                Bucket=bucket,
                Key=key)
            fp = response['Body'].get_raw_stream()
            yield fp

    def save(self, key, local_file):
        key = self._prefix + '/' + key
        self._client.upload_file(Bucket=self._bucket, LocalFilePath=local_file,
                                 Key=key)
        return self._bucket + '/' + key

    def save_data(self, key, lines):
        key = self._prefix + '/' + key
        data = BytesIO(gzip.compress('\n'.join(lines).encode('utf-8')))
        self._client.put_object(
            Bucket=self._bucket,
            Body=data,
            Key=key)
        return self._bucket + '/' + key
