# export DATA_SERVICE_URL="https://data-service.staging-bip-app.ssb.no"
export DATA_SERVICE_URL="http://127.0.0.1:8000"

#export GOOGLE_APPLICATION_CREDENTIALS=path/to/DATA_SERVICE_KEY_FILE.json

export STORAGE_ADAPTER='LOCAL'
export FILE_SERVICE_DATASTORE_ROOT_PREFIX="tests/resources"
export DATASTORE_ROOT="datastore_integration_test"

# export STORAGE_ADAPTER='GCS'
# export DATASTORE_ROOT="no_ssb_test"

export PYTHONUNBUFFERED=1

uvicorn application:data_service_app --reload
