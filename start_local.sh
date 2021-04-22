cd data_service

export BUCKET_NAME="data-service-bucket-microdata-poc"

# export DATA_SERVICE_URL="https://data-service.staging-bip-app.ssb.no"
export DATA_SERVICE_URL="http://127.0.0.1:8000"

export DATASTORE_ROOT="no_ssb_test"

export GOOGLE_APPLICATION_CREDENTIALS=/hers/goes/your/KEYFILE.json

export STORAGE_ADAPTER = 'LOCAL'

uvicorn main:data_service_app --reload
