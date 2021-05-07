# microdata-data-service
Receives POST request with dataset name and filter and creates a result set binary file.

Receives GET request for downloading binary file.


API documentation:
````
http://127.0.0.1:8000/docs
http://127.0.0.1:8000/redoc
````

## Docker image
````
docker build --tag data-service .
````
To run the application with Docker and datastore that is under /path/datastore/<DATASTORE_ROOT>.

Replace /path/datastore with absolute path to parent of DATASTORE_ROOT.
````
docker run --publish 8000:8000 --env CONFIG_PROFILE=LOCAL --env FILE_SERVICE_DATASTORE_ROOT_PREFIX=/datastore_path -v /path/datastore:/datastore_path data-service
````

## Running tests
Open terminal and go to root directory of the project and run:
````
pytest
````

## Configuration

There are currently two configuration profiles: LOCAL and GCS. You need to set environment variable CONFIG_PROFILE to 
one of the values. In case of GCS you need to have GOOGLE_APPLICATION_CREDENTIALS in environment pointing to JSON key file. 

The configuration files are ```data_service/config/.env.gcs``` and ```data_service/config/.env.local_file```.

## Running application from command line
```
% pwd
/microdata-data-service

% ./start_local.sh 
```
Pay attention to environment variables set in start_local.sh

## Running/debugging application in IntelliJ IDEA
Go to Edit configurations... -> Add New Configuration -> Python.

Set "Script path" to `[your-path]/application.py`

Set "Working directory" to `[your-path]/microdata-data-service`

Check [Configuration](#Configuration) for key file.

## Running/debugging tests in IntelliJ IDEA
Go to Edit configurations... -> Add New Configuration -> Python tests -> pytest.