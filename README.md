# microdata-data-service
Receives POST request with dataset name and filter and creates a result set binary file.

Receives GET request for downloading binary file.


API documentation:
````
http://127.0.0.1:8000/docs
http://127.0.0.1:8000/redoc
````

## Contribute

### Set up
To work on this repository you need to install [poetry](https://python-poetry.org/docs/):
```
# macOS / linux / BashOnWindows
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

# Windows powershell
(Invoke-WebRequest -Uri https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py -UseBasicParsing).Content | python -
```
Then install the virtual environment from the root directory:
```
poetry install
```

#### Intellij IDEA
Use plugin Poetry and add Python Interpreter "Poetry Environment". See https://plugins.jetbrains.com/plugin/14307-poetry

### Running tests
Open terminal and go to root directory of the project and run:
````
poetry run pytest --cov=data_service/
````

### Build docker image
````
docker build --tag data-service .
````
To run the application with Docker and datastore that is under /path/datastore/<DATASTORE_ROOT>.

Replace /path/datastore with absolute path to parent of DATASTORE_ROOT.
````
docker run --publish 8000:8000 --env CONFIG_PROFILE=LOCAL --env DATASTORE_DIR=/datastore_path --env RESULTSET_DIR=/resultset_path -v /path/datastore:/datastore_path -v /path/resultset:/resultset_path data-service
````

## Configuration

There are currently two configuration profiles: LOCAL and GCS. You need to set environment variable CONFIG_PROFILE to one of the values. In case of GCS you need to have GOOGLE_APPLICATION_CREDENTIALS in environment pointing to JSON key file. 

The configuration files are ```data_service/config/.env.gcs``` and ```data_service/config/.env.local_file```.

## Running application from command line
```
poetry run uvicorn application:data_service_app --reload
```

## Running/debugging application in IntelliJ IDEA
Go to Edit configurations... -> Add New Configuration -> Python.

Set "Script path" to `[your-path]/application.py`

Set "Working directory" to `[your-path]/microdata-data-service`

Check [Configuration](#Configuration) for key file.

## Running/debugging tests in IntelliJ IDEA
Go to Edit configurations... -> Add New Configuration -> Python tests -> pytest.

## Built with
* [Poetry](https://python-poetry.org/) - Python dependency and package management
* [uvicorn](https://www.uvicorn.org/) - Python ASGI-server
* [FastAPI](https://fastapi.tiangolo.com/) - Web framework
* [PyArrow](https://arrow.apache.org/docs/python/) - Apache Arrow
* [Pandas](https://pandas.pydata.org/) - Data analysis and manipulation
* [Numpy](https://numpy.org/) - Scientific computing
* [google-cloud-storage](https://googleapis.dev/python/storage/latest/client.html) - Client for interacting with the Google Cloud Storage API
