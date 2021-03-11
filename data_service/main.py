import uuid
from datetime import date
from typing import Optional

import pyarrow.parquet as pq
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel


class InputQuery(BaseModel):
    dataStructureName: str
    version: str
    startDate: str
    stopDate: str
    values: Optional[list]
    population: Optional[dict]
    interval_filter: Optional[str]
    # credentials: Credentials <-TODO
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

    filename = input_query.dataStructureName + '__1_0.parquet'
    print('Parquet metadata: ' + str(pq.read_metadata(filename)))
    print('Parquet schema: ' + pq.read_schema(filename).to_string())

    data = pq.read_table(source=filename, filters=[('start', '>=', start), ('stop', '<=', stop)])

    result_filename = str(uuid.uuid4()) + '.parquet'
    # print('Resultset: ' + str(data.to_pandas().head(50)))
    pq.write_table(data, result_filename)
    print('Parquet metadata of result set: ' + str(pq.read_metadata(result_filename)))

    return {'name': input_query.dataStructureName,
            'dataUrl': 'http://localhost:8000/retrieveResultSet?file_name=' + result_filename}


if __name__ == "__main__":
    uvicorn.run(data_service_app, host="0.0.0.0", port=8000)
