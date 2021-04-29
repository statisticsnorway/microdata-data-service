import logging

import uvicorn
from fastapi import FastAPI
from starlette.responses import PlainTextResponse

from data_service.api.data_api import data_router
from data_service.api.observability_api import observability_router

data_service_app = FastAPI()

data_service_app.include_router(data_router)
data_service_app.include_router(observability_router)


@data_service_app.exception_handler(Exception)
async def validation_exception_handler(request, exc):
    log = logging.getLogger(__name__)
    log.exception(exc)
    return PlainTextResponse("Internal Server Error", status_code=500)


@data_service_app.on_event("startup")
async def startup_event():
    log = logging.getLogger(__name__)
    log.info('Started data-service')


if __name__ == "__main__":
    uvicorn.run(data_service_app, host="0.0.0.0", log_config='data_service/config/logging.yaml', port=8000)
