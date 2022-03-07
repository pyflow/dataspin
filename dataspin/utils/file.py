
import os
import json
import gzip
import tempfile
from basepy.log import logger
import boto3
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client


class DataFileReader:
    def __init__(self, file_path, **kwargs):
        self.file_path = file_path
        self.ext = os.path.splitext(self.file_path)[-1]
        self.auth_info = kwargs.get('auth_info')

    def readlines(self):
        if not self.auth_info:
            file_reader = LocalDataFileReader(self.file_path)
            for data in file_reader.readlines():
                yield data
        else:
            access_key = self.auth_info['access_key']
            secret_key = self.auth_info['secret_key']
            region = self.auth_info['region']
            bucket, key = self.file_path.split('/', 1)
            if self.auth_info.get('source') == 's3':
                file_reader = S3DataFileReader(access_key, secret_key, region)
            elif self.auth_info.get('source') == 'cos':
                file_reader = COSDataFileReader(access_key, secret_key, region)
            else:
                raise Exception('Not support Storage')
            for data in file_reader.readlines(bucket, key, self.ext):
                yield data


class LocalDataFileReader:

    def __init__(self, file_path, **kwargs):
        self.file_path = file_path
        self.ext = os.path.splitext(self.file_path)[-1]

    def readlines(self):
        if self.ext == '.gz':
            with gzip.open(self.file_path, mode='rt', encoding='utf-8') as gdata:
                for line in gdata:
                    yield (json.loads(line), line.strip())
        else:
            with open(self.file_path, 'rt') as f:
                for line in f.readlines():
                    yield (json.loads(line), line.strip())


class COSDataFileReader:

    def __init__(self, access_key, secret_key, region):
        config = CosConfig(Region=region, SecretId=access_key,
                           SecretKey=secret_key, Token=None, Scheme='https')
        self._client = CosS3Client(config)

    def readlines(self, bucket, key, file_type='.gz'):
        if file_type == '.gz':
            for data in self.read_gzfile(bucket, key):
                yield data
        elif file_type == '.jsonl':
            for data in self.read_jsonl(bucket, key):
                yield data
        else:
            raise Exception('No support file type=%s' % file_type)

    def read_gzfile(self, bucket, key):
        with tempfile.TemporaryFile('w+b') as fp:
            response = self.client.get_object(
                Bucket=bucket,
                Key=key)
            fp = response['Body'].get_raw_stream()
            fp.seek(0)
            try:
                with gzip.open(fp, mode='rt', encoding='utf-8') as gdata:
                    for line in gdata:
                        try:
                            yield (json.loads(line), line.strip())
                        except Exception as e:
                            logger.error(
                                'parse jsonl data error,bucket={}, key={}, data={}, error={}'.format(
                                    bucket, key, line, e))
            except Exception as e:
                logger.error(
                    'read cos file error, bucket={}, key={}, error={}'.format(
                        bucket, key, e))

    def read_jsonl(self, bucket, key):
        with tempfile.TemporaryFile('w+b') as fp:
            response = self.client.get_object(
                Bucket=bucket,
                Key=key)
            fp = response['Body'].get_raw_stream()
            fp.seek(0)
            try:
                for line in fp:
                    try:
                        yield (json.loads(line), line.strip())
                    except Exception as e:
                        logger.error(
                            'parse jsonl data error,bucket={}, key={}, data={}, error={}'.format(
                                bucket, key, line, e))
            except Exception as e:
                logger.error(
                    'read cos file error, bucket={}, key={}, error={}'.format(
                        bucket, key, e))


class S3DataFileReader:

    def __init__(self, access_key, secret_key, region):
        self._s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )

    def readlines(self, bucket, key, file_type='.gz'):
        if file_type == '.gz':
            for data in self.read_gzfile(bucket, key):
                yield data
        elif file_type == '.jsonl':
            for data in self.read_jsonl(bucket, key):
                yield data
        else:
            raise Exception('No support file type=%s' % file_type)

    def read_gzfile(self, bucket, key):
        with tempfile.TemporaryFile('w+b') as data:
            self._s3_client.download_fileobj(bucket, key, data)
            data.seek(0)
            try:
                with gzip.open(data, mode='rt', encoding='utf-8') as gdata:
                    for line in gdata:
                        try:
                            yield (json.loads(line), line.strip())
                        except Exception as e:
                            logger.error(
                                'parse jsonl data error,bucket={}, key={}, data={}, error={}'.format(
                                    bucket, key, line, e))
                            raise e
            except Exception as e:
                logger.error(
                    'read s3 file error, bucket={}, key={}, error={}'.format(
                        bucket, key, e))
                raise e

    def read_jsonl(self, bucket, key):
        with tempfile.TemporaryFile('w+b') as fp:
            self._s3_client.download_fileobj(bucket, key, fp)
            fp.seek(0)
            try:
                for line in fp:
                    yield (json.loads(line), line.strip())
            except Exception as e:
                logger.error(
                    'read s3 file error, bucket={}, key={}, error={}'.format(
                        bucket, key, e))
                raise e
