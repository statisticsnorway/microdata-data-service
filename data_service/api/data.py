import os
import uuid
from datetime import date

import pyarrow.parquet as pq
import sys
from fastapi import APIRouter
from fastapi.responses import FileResponse
from google.cloud import storage

from data_service.api.models import InputQuery

data_router = APIRouter()


@data_router.get("/retrieveResultSet")
def retrieve_result_set(file_name: str):
    # TODO OAuth2
    """
    Retrieve a result set:

    - **file_name**: UUID of the file generated
    """
    return FileResponse(path=file_name, filename=file_name, media_type='application/octet-stream')


@data_router.post("/data/event")
def create_result_set_event_data(input_query: InputQuery):
    """
     Create result set of data with temporality type event.

     - **input_query**: InputQuery as JSON
     """
    start = date.fromtimestamp(int(input_query.startDate) * 3600 * 24)
    stop = date.fromtimestamp(int(input_query.stopDate) * 3600 * 24)
    print('Start date: ' + str(start))
    print('Stop date: ' + str(stop))

    # TODO config like Spring profiles (dev, prod)
    # parquet_file = download_file_from_storage(input_query.dataStructureName)
    parquet_file = input_query.dataStructureName + '__1_0.parquet'

    print('Parquet metadata: ' + str(pq.read_metadata(parquet_file)))
    print('Parquet schema: ' + pq.read_schema(parquet_file).to_string())

    data = pq.read_table(source=parquet_file, filters=[('start', '>=', start), ('stop', '<=', stop)])
    size = sys.getsizeof(data)
    print('Size of filtered pyarrow table: ' + str(size) + ' bytes (' + str(size/1000000) + ' MB)')

    result_filename = str(uuid.uuid4()) + '.parquet'
    # print('Resultset: ' + str(data.to_pandas().head(50)))
    pq.write_table(data, result_filename)
    print('Parquet metadata of result set: ' + str(pq.read_metadata(result_filename)))
    print('Size of file with result set: ' + str(os.path.getsize(result_filename)/1000000) + ' MB')

    return {'name': input_query.dataStructureName,
            'dataUrl': getenv('DATA_SERVICE_URL') + '/retrieveResultSet?file_name=' + result_filename}


def download_file_from_storage(datastucture_name: str) -> str:
    download_filename = datastucture_name + '__1_0.parquet'
    bucket_name = getenv('BUCKET_NAME')
    blob_download_path = create_download_path(datastucture_name)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_download_path)
    blob.download_to_filename(download_filename)
    print("Downloaded blob {} to {} from bucket {}.".format(blob_download_path, download_filename, bucket_name))
    return download_filename


def create_download_path(datastructure_name: str) -> str:
    path = getenv('DATASTORE_ROOT') + '/dataset/' + datastructure_name + '/' + datastructure_name + '__1_0.parquet'
    print ("Trying to download blob {}".format(path))
    return path


def getenv(key: str) -> str:
    return os.getenv(key, key + ' does not exist')
