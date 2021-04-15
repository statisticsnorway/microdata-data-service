import os
import uuid
from datetime import date

import pyarrow.parquet as pq
import sys
from fastapi import APIRouter
from fastapi.responses import FileResponse

from data_service import getenv
from data_service.api.models import InputQuery
from data_service.service.gcs_service import GcsFileService
from data_service.service.local_file_service import LocalFileService
from data_service.service.service import FileService

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
    file_service: FileService = GcsFileService()
    # file_service: FileService = LocalFileService()
    parquet_file = file_service.get_file(path=input_query.dataStructureName)

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
