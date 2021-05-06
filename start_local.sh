#Needed for Google Cloud Storage access
#export GOOGLE_APPLICATION_CREDENTIALS=path/to/DATA_SERVICE_KEY_FILE.json

export CONFIG_PROFILE='LOCAL'
# export CONFIG_PROFILE='GCS'

export PYTHONUNBUFFERED=1

uvicorn application:data_service_app --reload
