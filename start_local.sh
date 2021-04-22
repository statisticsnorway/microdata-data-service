# export DATA_SERVICE_URL="https://data-service.staging-bip-app.ssb.no"
export DATA_SERVICE_URL="http://127.0.0.1:8000"

export GOOGLE_APPLICATION_CREDENTIALS=/here/goes/your/KEYFILE.json

export STORAGE_ADAPTER = 'LOCAL'

uvicorn application:data_service_app --reload
