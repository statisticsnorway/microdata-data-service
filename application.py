import logging

import json_logging
import uvicorn
from fastapi import FastAPI
from starlette.responses import PlainTextResponse

from data_service.api.data_api import data_router
from data_service.api.observability_api import observability_router

from data_service.config import config

data_service_app = FastAPI()

data_service_app.include_router(data_router)
data_service_app.include_router(observability_router)


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


@data_service_app.exception_handler(Exception)
async def validation_exception_handler(request, exc):
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
