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
docker run --publish 8000:8000 data-service
````
