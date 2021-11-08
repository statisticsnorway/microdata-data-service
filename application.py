import logging

import json_logging
from fastapi.encoders import jsonable_encoder
import uvicorn
from fastapi import FastAPI, status
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.staticfiles import StaticFiles

from fastapi.responses import JSONResponse
from starlette.responses import PlainTextResponse, Response

from data_service.api.data_api import data_router
from data_service.api.observability_api import observability_router
from data_service.config import config
from data_service.core.processor import (
    NotFoundException, EmptyResultSetException
)

"""
    Self-hosting JavaScript and CSS for docs
    https://fastapi.tiangolo.com/advanced/extending-openapi/#self-hosting-javascript-and-css-for-docs
"""
data_service_app = FastAPI(docs_url=None, redoc_url=None)
data_service_app.mount("/static", StaticFiles(directory="static"), name="static")

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


@data_service_app.get(data_service_app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@data_service_app.get("/redoc", include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=data_service_app.openapi_url,
        title=data_service_app.title + " - ReDoc",
        redoc_js_url="/static/redoc.standalone.js",
    )


class CustomJSONLog(json_logging.JSONLogFormatter):
    """
    Customized logger
    """

    def _format_log_object(self, record, request_util):
        json_log_object = super(CustomJSONLog, self)._format_log_object(record, request_util)

        json_log_object.update({
            "message": record.getMessage()
        })

        json_log_object["@timestamp"] = json_log_object.pop('written_at')

        del json_log_object['msg']

        return json_log_object


@data_service_app.exception_handler(EmptyResultSetException)
async def empty_result_set_exception_handler(request, exc):
    log = logging.getLogger(__name__)
    log.exception(exc)
    return Response(
        status_code=status.HTTP_204_NO_CONTENT
    )


@data_service_app.exception_handler(NotFoundException)
async def not_found_exception_handler(request, exc):
    log = logging.getLogger(__name__)
    log.exception(exc)
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content=jsonable_encoder({"detail": "No such datastructure"})
    )


@data_service_app.exception_handler(Exception)
async def unknown_exception_handler(request, exc):
    log = logging.getLogger(__name__)
    log.exception(exc)
    return PlainTextResponse("Internal Server Error", status_code=500)


@data_service_app.on_event("startup")
def startup_event():
    json_logging.init_fastapi(enable_json=True, custom_formatter=CustomJSONLog)
    json_logging.init_request_instrument(data_service_app)

    logging.basicConfig(level=logging.INFO)
    json_logging.config_root_logger()

    log = logging.getLogger(__name__)

    log.info('Started data-service')
    log.info(config.get_settings().print())


if __name__ == "__main__":
    uvicorn.run(data_service_app, host="0.0.0.0", port=8000)
