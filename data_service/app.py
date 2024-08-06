# pylint: disable=unused-argument
import logging

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
from starlette.responses import PlainTextResponse

from data_service.config import environment
from data_service.api.data_api import data_router
from data_service.api.observability_api import observability_router
from data_service.config.uvicorn import setup_uvicorn_logging
from data_service.config.logging_config import setup_logging
from data_service.exceptions import NotFoundException

# Self-hosting JavaScript and CSS for docs
# https://fastapi.tiangolo.com/advanced/extending-openapi/#self-hosting-javascript-and-css-for-docs

DESCRIPTION = """
The Parquet file format returned or produced by this service reported by \
`pq.read_schema(parquet_file).to_string()` is as follows:
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
data_service_app = FastAPI(title="Data service", description=DESCRIPTION)
data_service_app.mount(
    "/static", StaticFiles(directory="static"), name="static"
)

data_service_app.include_router(data_router)
data_service_app.include_router(observability_router)

logger = logging.getLogger()
setup_logging(data_service_app)
setup_uvicorn_logging()


@data_service_app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=data_service_app.openapi_url,
        title=data_service_app.title + " - Swagger UI",
        oauth2_redirect_url=data_service_app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="/static/swagger-ui-bundle.js",
        swagger_css_url="/static/swagger-ui.css",
    )


@data_service_app.get(
    data_service_app.swagger_ui_oauth2_redirect_url, include_in_schema=False
)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@data_service_app.get("/redoc", include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=data_service_app.openapi_url,
        title=data_service_app.title + " - ReDoc",
        redoc_js_url="/static/redoc.standalone.js",
    )


@data_service_app.exception_handler(NotFoundException)
async def not_found_exception_handler(exc):
    logger.exception(exc)
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content=jsonable_encoder({"detail": "No such datastructure"}),
    )


@data_service_app.exception_handler(Exception)
async def unknown_exception_handler(exc):
    logger.exception(exc)
    return PlainTextResponse("Internal Server Error", status_code=500)


if __name__ == "__main__":
    uvicorn.run(
        data_service_app,
        host="0.0.0.0",
        port=environment.get("PORT"),
    )
