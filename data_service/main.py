import uuid
from datetime import date
from typing import Optional

import pyarrow.parquet as pq
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from google.cloud import storage
from pydantic import BaseModel


class InputQuery(BaseModel):
    dataStructureName: str
    version: str
    startDate: str
    stopDate: str
    values: Optional[list]
    population: Optional[dict]
    interval_filter: Optional[str]
    include_attributes: Optional[bool] = False


data_service_app = FastAPI()


@data_service_app.get("/retrieveResultSet")
def retrieve_result_set(file_name: str):
    # TODO OAuth2
    """
    Retrieve a result set:

    - **file_name**: UUID of the file generated
    """
    return FileResponse(path=file_name, filename=file_name, media_type='application/octet-stream')


@data_service_app.post("/data/event")
def create_result_set_event_data(input_query: InputQuery):
    """
     Create result set of data with temporality type event.

     - **input_query**: InputQuery as JSON
     """
    start = date.fromtimestamp(int(input_query.startDate) * 3600 * 24)
    stop = date.fromtimestamp(int(input_query.stopDate) * 3600 * 24)
    print('Start date: ' + str(start))
    print('Stop date: ' + str(stop))

    filename_from_query = input_query.dataStructureName + '__1_0.parquet'
    downloaded_filename = download_file_from_storage(filename_from_query)

    print('Parquet metadata: ' + str(pq.read_metadata(downloaded_filename)))
    print('Parquet schema: ' + pq.read_schema(downloaded_filename).to_string())

    data = pq.read_table(source=downloaded_filename, filters=[('start', '>=', start), ('stop', '<=', stop)])

    result_filename = str(uuid.uuid4()) + '.parquet'
    # print('Resultset: ' + str(data.to_pandas().head(50)))
    pq.write_table(data, result_filename)
    print('Parquet metadata of result set: ' + str(pq.read_metadata(result_filename)))

    return {'name': input_query.dataStructureName,
            'dataUrl': 'https://data-service.staging-bip-app.ssb.no/retrieveResultSet?file_name=' + result_filename}


@data_service_app.get('/health/alive')
def alive():
    return "I'm alive!"


@data_service_app.get('/health/ready')
def ready():
    return "I'm ready!"


def download_file_from_storage(filename_from_query: str) -> str:
    bucket_name = 'data-service-bucket-microdata-poc'
    destination_file_name = filename_from_query
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(filename_from_query)
    blob.download_to_filename(destination_file_name)
    print("Blob {} from bucket {} downloaded to {}.".format(filename_from_query, bucket_name, destination_file_name))
    return destination_file_name


if __name__ == "__main__":
    uvicorn.run(data_service_app, host="0.0.0.0", port=8000)
