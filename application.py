import uvicorn
from fastapi import FastAPI

from data_service.api.data_api import data_router
from data_service.api.observability_api import observability_router
from data_service.config.logging import get_logger

log = get_logger(__name__)

data_service_app = FastAPI()

data_service_app.include_router(data_router)
data_service_app.include_router(observability_router)

log.info('Started data-service')

if __name__ == "__main__":
    uvicorn.run(data_service_app, host="0.0.0.0", port=8000)
