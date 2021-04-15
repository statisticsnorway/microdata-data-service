import uvicorn
from fastapi import FastAPI

from data_service.api.data import data_router
from data_service.api.observability import observability_router

data_service_app = FastAPI()

data_service_app.include_router(data_router)
data_service_app.include_router(observability_router)

if __name__ == "__main__":
    uvicorn.run(data_service_app, host="0.0.0.0", port=8000)
