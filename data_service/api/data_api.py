import logging
import os

from fastapi import APIRouter, Depends, Header
from fastapi.responses import FileResponse
from fastapi import HTTPException, status

from data_service.api.query_models import (
    InputTimePeriodQuery, InputTimeQuery, InputFixedQuery
)
from data_service.config import config
from data_service.config.config import get_settings
from data_service.config.dependencies import get_processor
from data_service.core.processor import Processor
from data_service.api.auth import authorize_user

data_router = APIRouter()


@data_router.get("/data/resultSet")
def retrieve_result_set(file_name: str,
                        authorization: str = Header(None),
                        settings: config.BaseSettings = Depends(get_settings)):
    """
    Retrieve a result set:

    - **file_name**: UUID of the file generated
    - **settings**: config.Settings object
    - **authorization**: JWT token authorization header
    """
    log = logging.getLogger(__name__)
    log.info(
        f"Entering /data/resultSet with request for file name: {file_name}"
    )

    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    file_path = (
        f"{settings.RESULTSET_DIR}/{file_name}"
    )
    if not os.path.isfile(file_path):
        log.warn(f"No file found for path: {file_path}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Result set not found'
        )
    else:
        return FileResponse(
            file_path, media_type='application/octet-stream'
        )


@data_router.post("/data/event")
def create_result_set_event_data(input_query: InputTimePeriodQuery,
                                 authorization: str = Header(None),
                                 settings: config.BaseSettings = Depends(get_settings),
                                 processor: Processor = Depends(get_processor)):
    """
     Create result set of data with temporality type event.

     - **input_query**: InputTimePeriodQuery as JSON
     - **settings**: config.Settings object
     - **authorization**: JWT token authorization header
     """
    log = logging.getLogger(__name__)
    log.info(f'Entering /data/event with input query: {input_query}')

    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    resultset_file_name = processor.process_event_request(input_query)
    resultset_data_url = (
        f"{settings.DATA_SERVICE_URL}/data/resultSet"
        f"?file_name={resultset_file_name}"
    )
    log.info(f'Data url for event result set: {resultset_data_url}')

    return {
        'name': input_query.dataStructureName,
        'dataUrl': resultset_data_url
    }


@data_router.post("/data/status")
def create_result_set_status_data(input_query: InputTimeQuery,
                                  authorization: str = Header(None),
                                  settings: config.BaseSettings = Depends(get_settings),
                                  processor: Processor = Depends(get_processor)):
    """
     Create result set of data with temporality type status.

     - **input_query**: InputTimeQuery as JSON
     - **settings**: config.Settings object
     - **authorization**: JWT token authorization header
     """
    log = logging.getLogger(__name__)
    log.info(f'Entering /data/status with input query: {input_query}')

    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    resultset_file_name = processor.process_status_request(input_query)
    resultset_data_url = (
        f"{settings.DATA_SERVICE_URL}/data/resultSet"
        f"?file_name={resultset_file_name}"
    )
    log.info(f'Data url for status result set: {resultset_data_url}')

    return {
        'name': input_query.dataStructureName,
        'dataUrl': resultset_data_url
    }


@data_router.post("/data/fixed")
def create_result_set_fixed_data(input_query: InputFixedQuery,
                                 authorization: str = Header(None),
                                 settings: config.BaseSettings = Depends(get_settings),
                                 processor: Processor = Depends(get_processor)):
    """
     Create result set of data with temporality type fixed.

     - **input_query**: InputFixedQuery as JSON
     - **settings**: config.Settings object
     - **authorization**: JWT token authorization header
     """
    log = logging.getLogger(__name__)
    log.info(f'Entering /data/fixed with input query: {input_query}')

    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    resultset_file_name = processor.process_fixed_request(input_query)
    resultset_data_url = (
        f"{settings.DATA_SERVICE_URL}/data/resultSet"
        f"?file_name={resultset_file_name}"
    )
    log.info(f'data url for fixed result set: {resultset_data_url}')

    return {
        'name': input_query.dataStructureName,
        'dataUrl': resultset_data_url
    }

