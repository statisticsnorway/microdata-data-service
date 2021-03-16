cd data_service
export BUCKET_NAME="data-service-bucket-microdata-poc"
export DATA_SERVICE_URL="https://data-service.staging-bip-app.ssb.no"
uvicorn main:data_service_app --reload
