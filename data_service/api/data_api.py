import logging

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse

from data_service.api.query_models import InputTimePeriodQuery, QueryValidator
from data_service.config import config
from data_service.config.config import get_settings
from data_service.core import processor

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
def create_result_set_event_data(input_query: InputTimePeriodQuery, settings: config.Settings = Depends(get_settings)):
    """
     Create result set of data with temporality type event.

     - **input_query**: InputQuery as JSON
     - **settings**: config.Settings object
     """
    log = logging.getLogger(__name__)
    log.info(f'Entering /data/event with input query: {input_query}')
    QueryValidator.validate(input_query)

    result_filename = processor.process(input_query, settings)
    log.info(f'Filename with the result set: {result_filename}')

    return {'name': input_query.dataStructureName,
            'dataUrl': settings.DATA_SERVICE_URL + '/retrieveResultSet?file_name=' + result_filename}
