cd data_service
export BUCKET_NAME="data-service-bucket-microdata-poc"
# export DATA_SERVICE_URL="https://data-service.staging-bip-app.ssb.no"
export DATA_SERVICE_URL="http://127.0.0.1:8000"
uvicorn main:data_service_app --reload
