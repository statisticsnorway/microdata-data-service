# pylint: disable=unused-argument
import logging

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, Header, Request
from fastapi.responses import PlainTextResponse

from data_service.api.auth import authorize_user
from data_service.domain import data
from data_service.api.response_models import ErrorMessage
from data_service.api.query_models import (
    InputTimePeriodQuery, InputTimeQuery, InputFixedQuery
)


data_router = APIRouter()
logger = logging.getLogger()


@data_router.post(
    '/data/event/stream', responses={404: {'model': ErrorMessage}}
)
def stream_result_event(
    input_query: InputTimePeriodQuery,
    request: Request,  # needed for json_logging.get_correlation_id
    authorization: str = Header(None),
):
    """
    Create Result set of data with temporality type event,
    and stream result as response.
    """
    logger.info(f'Entering /data/event/stream with input query: {input_query}')

    user_id = authorize_user(authorization)
    logger.info(f'Authorized token for user: {user_id}')

    result_data = data.process_event_request(input_query)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(
        buffer_stream.getvalue().to_pybytes()
    )


@data_router.post(
    '/data/status/stream', responses={404: {'model': ErrorMessage}}
)
def stream_result_status(
    input_query: InputTimeQuery,
    request: Request,  # needed for json_logging.get_correlation_id
    authorization: str = Header(None),
):
    """
    Create result set of data with temporality type status,
    and stream result as response.
    """
    logger.info(
        f'Entering /data/status/stream with input query: {input_query}'
    )
    user_id = authorize_user(authorization)
    logger.info(f'Authorized token for user: {user_id}')

    result_data = data.process_status_request(input_query)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(
        buffer_stream.getvalue().to_pybytes()
    )


@data_router.post(
    '/data/fixed/stream', responses={404: {'model': ErrorMessage}}
)
def stream_result_fixed(
    input_query: InputFixedQuery,
    request: Request,  # needed for json_logging.get_correlation_id
    authorization: str = Header(None),
):
    """
    Create result set of data with temporality type fixed,
    and stream result as response.
    """
    logger.info(f'Entering /data/fixed/stream with input query: {input_query}')
    user_id = authorize_user(authorization)
    logger.info(f'Authorized token for user: {user_id}')

    result_data = data.process_fixed_request(input_query)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(
        buffer_stream.getvalue().to_pybytes()
    )
