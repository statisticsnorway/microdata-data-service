import logging
import sys
import uuid

import json_logging
import uvicorn
from fastapi import FastAPI, status
from fastapi.encoders import jsonable_encoder
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response

from data_service.api.data_api import data_router
from data_service.api.observability_api import observability_router
from data_service.config.logging_config import \
    CustomJSONLog, CustomJSONRequestLogFormatter
from data_service.core.filters import EmptyResultSetException
from data_service.exceptions import NotFoundException

# Self-hosting JavaScript and CSS for docs
# https://fastapi.tiangolo.com/advanced/extending-openapi/#self-hosting-javascript-and-css-for-docs

DESCRIPTION = """
The Parquet file format returned or produced by this service reported by `pq.read_schema(parquet_file).to_string()` is as follows:
```
format_version: 1.0
unit_id: uint64
-- field metadata --
PARQUET:field_id: '1'
value: string | int64 | float64
-- field metadata --
PARQUET:field_id: '2'
start_epoch_days: int16
-- field metadata --
PARQUET:field_id: '3'
stop_epoch_days: int16
-- field metadata --
PARQUET:field_id: '4'
```
"""

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

data_service_app = FastAPI(
    title="Data service",
    description=DESCRIPTION
)
data_service_app.mount(
    "/static",
    StaticFiles(directory="static"),
    name="static"
)

data_service_app.include_router(data_router)
data_service_app.include_router(observability_router)


@data_service_app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=data_service_app.openapi_url,
        title=data_service_app.title + " - Swagger UI",
        oauth2_redirect_url=data_service_app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="/static/swagger-ui-bundle.js",
        swagger_css_url="/static/swagger-ui.css",
    )


@data_service_app.get(data_service_app.swagger_ui_oauth2_redirect_url,
                      include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@data_service_app.get("/redoc",
                      include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=data_service_app.openapi_url,
        title=data_service_app.title + " - ReDoc",
        redoc_js_url="/static/redoc.standalone.js",
    )


@data_service_app.exception_handler(EmptyResultSetException)
async def empty_result_set_exception_handler(
        request,  # pylint: disable=unused-argument
        exc):  # pylint: disable=unused-argument
    logger.info("Empty result set.")
    return Response(
        status_code=status.HTTP_204_NO_CONTENT
    )


@data_service_app.exception_handler(NotFoundException)
async def not_found_exception_handler(
        request,  # pylint: disable=unused-argument
        exc):
    logger.exception(exc)
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content=jsonable_encoder({"detail": "No such datastructure"})
    )


@data_service_app.exception_handler(Exception)
async def unknown_exception_handler(
        request,  # pylint: disable=unused-argument
        exc):
    logger.exception(exc)
    return PlainTextResponse("Internal Server Error", status_code=500)


@data_service_app.middleware("http")
async def add_x_request_id_response_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Request-ID"] = \
        json_logging.get_correlation_id(request=request)
    return response


@data_service_app.on_event("startup")
def startup_event():
    json_logging.CREATE_CORRELATION_ID_IF_NOT_EXISTS = True
    json_logging.CORRELATION_ID_GENERATOR = \
        lambda: "data-service-" + str(uuid.uuid1())
    json_logging.init_fastapi(enable_json=True, custom_formatter=CustomJSONLog)
    json_logging.init_request_instrument(
        data_service_app,
        custom_formatter=CustomJSONRequestLogFormatter
    )


if __name__ == "__main__":
    uvicorn.run(data_service_app, host="0.0.0.0", port=8000)
