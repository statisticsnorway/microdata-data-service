import logging

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, Header
from fastapi.responses import PlainTextResponse

from data_service.api.auth import authorize_user
from data_service.api.query_models import (
    InputTimePeriodQuery, InputTimeQuery, InputFixedQuery
)
from data_service.api.response_models import ErrorMessage
from data_service.config.dependencies import get_processor
from data_service.core.processor import Processor

data_router = APIRouter()
log = logging.getLogger(__name__)


@data_router.post("/data/event/generate-file",
                  responses={404: {"model": ErrorMessage}})
def create_result_file_event(input_query: InputTimePeriodQuery,
                             authorization: str = Header(None),
                             processor: Processor = Depends(get_processor)):
    """
    Create result set of data with temporality type event,
    and write result to file. Returns name of file in response.
    """
    log.info(
        f'Entering /data/event/generate-file with input query: {input_query}'
    )

    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    result_data = processor.process_event_request(input_query)
    resultset_file_name = processor.write_table(result_data)
    log.info(f'File name for event result set: {resultset_file_name}')

    return {
        'filename': resultset_file_name,
    }


@data_router.post("/data/status/generate-file",
                  responses={404: {"model": ErrorMessage}})
def create_result_file_status(input_query: InputTimeQuery,
                              authorization: str = Header(None),
                              processor: Processor = Depends(get_processor)):
    """
    Create result set of data with temporality type status,
    and write result to file. Returns name of file in response.
    """
    log.info(
        f'Entering /data/status/generate-file with input query: {input_query}'
    )

    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    result_data = processor.process_status_request(input_query)
    resultset_file_name = processor.write_table(result_data)
    log.info(f'File name for event result set: {resultset_file_name}')

    return {
        'filename': resultset_file_name,
    }


@data_router.post("/data/fixed/generate-file",
                  responses={404: {"model": ErrorMessage}})
def create_file_result_fixed(input_query: InputFixedQuery,
                             authorization: str = Header(None),
                             processor: Processor = Depends(get_processor)):
    """
    Create result set of data with temporality type fixed,
    and write result to file. Returns name of file in response.
    """
    log.info(
        f'Entering /data/fixed/generate-file with input query: {input_query}'
    )

    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    result_data = processor.process_fixed_request(input_query)
    resultset_file_name = processor.write_table(result_data)
    log.info(f'File name for event result set: {resultset_file_name}')

    return {
        'filename': resultset_file_name,
    }


@data_router.post("/data/event/stream",
                  responses={404: {"model": ErrorMessage}})
def stream_result_event(input_query: InputTimePeriodQuery,
                        authorization: str = Header(None),
                        processor: Processor = Depends(get_processor)):
    """
    Create Result set of data with temporality type event,
    and stream result as response.
    """
    log.info(f'Entering /data/event/stream with input query: {input_query}')

    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    result_data = processor.process_event_request(input_query)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(
        buffer_stream.getvalue().to_pybytes()
    )


@data_router.post("/data/status/stream",
                  responses={404: {"model": ErrorMessage}})
def stream_result_status(input_query: InputTimeQuery,
                         authorization: str = Header(None),
                         processor: Processor = Depends(get_processor)):
    """
    Create result set of data with temporality type status,
    and stream result as response.
    """
    log.info(f'Entering /data/status/stream with input query: {input_query}')

    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    result_data = processor.process_status_request(input_query)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(
        buffer_stream.getvalue().to_pybytes()
    )


@data_router.post("/data/fixed/stream",
                  responses={404: {"model": ErrorMessage}})
def stream_result_fixed(input_query: InputFixedQuery,
                        authorization: str = Header(None),
                        processor: Processor = Depends(get_processor)):
    """
    Create result set of data with temporality type fixed,
    and stream result as response.
    """
    log.info(f'Entering /data/fixed/stream with input query: {input_query}')
    user_id = authorize_user(authorization)
    log.info(f"Authorized token for user: {user_id}")

    result_data = processor.process_fixed_request(input_query)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(
        buffer_stream.getvalue().to_pybytes()
    )
