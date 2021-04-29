# microdata-data-service
Receives POST request with dataset name and filter and creates a result set binary file.

Receives GET request for downloading binary file.

To start the service:
```
% pwd
/microdata-data-service

% ./start_local.sh 
```

API documentation:
````
http://127.0.0.1:8000/docs
http://127.0.0.1:8000/redoc
````

## Docker image
````
docker build --tag data-service .
docker run --publish 8000:8000 --env STORAGE_ADAPTER=LOCAL data-service
````

## Running tests
Open terminal and go to root directory of the project and run:
````
pytest
````

## Running/debugging application in IntelliJ IDEA
Go to Edit configurations... -> Add New Configuration -> Python.

Set "Script path" to `[your-path]/application.py`

Set "Working directory" to `[your-path]/microdata-data-service`

## Running/debugging tests in IntelliJ IDEA
Go to Edit configurations... -> Add New Configuration -> Python tests -> pytest.