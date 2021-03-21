import os
import pytest
import gcsfs
from xialib_gcs import GcsStorer


@pytest.fixture(scope='module')
def storer():
    storer = GcsStorer()
    yield storer

def test_simple_flow(storer: GcsStorer):
    storer.mkdir(storer.join("gs://xialib-gcp-test", "gcs-storer"))
    dir_path = storer.join("gs://xialib-gcp-test", "gcs-storer")
    file_path = storer.join("gs://xialib-gcp-test", "gcs-storer", "schema.json")
    dest_file = storer.join("gs://xialib-gcp-test", "gcs-storer", "schema1.json")
    assert storer.exists(file_path)

    data_copy1 = storer.read(file_path)
    for data_io in storer.get_io_stream(file_path):
        new_file = storer.write(data_io, dest_file)
        assert new_file == dest_file

    for data_io in storer.get_io_stream(file_path):
        for write_io in storer.get_io_wb_stream(dest_file):
            write_io.write(data_io.read())

    for data_io in storer.get_io_stream(dest_file):
        data_copy2 = data_io.read()
        assert data_copy1 == data_copy2
    assert storer.remove(dest_file)
    assert not storer.remove(dest_file)

    storer.write(data_copy1, dest_file)

    counter = 0
    for file_item in storer.walk_file(dir_path):
        assert storer.exists(file_item)
        counter += 1
    assert counter == 3

    storer.remove(dest_file)

def test_init_2():
    storer = GcsStorer(fs=gcsfs.GCSFileSystem())

def test_exceptions():
    with pytest.raises(TypeError):
        storer = GcsStorer(fs=object())