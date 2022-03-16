from dataspin.data import DataFileMessage


def test_data_file_message_1():
    m = DataFileMessage(file_url="file:///path/to/file.json")
    assert m.bucket == ''
    assert m.path == '/path/to/file.json'
    assert m.storage_type == 'file'
    m = DataFileMessage(file_url="file://c:/path/to/file.json")
    assert m.bucket == ''
    assert m.path == 'c:/path/to/file.json'
    assert m.storage_type == 'file'
    m = DataFileMessage(file_url="s3://bucket_name/path/to/file.json")
    assert m.bucket == 'bucket_name'
    assert m.path == '/path/to/file.json'
    assert m.storage_type == 's3'