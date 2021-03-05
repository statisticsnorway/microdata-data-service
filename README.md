# microdata-data-service
Receives POST request with dataset name and filter and creates a result set binary file.

Receives GET request for downloading binary file.

To start the service:
```
% pwd
/microdata-data-service/data-service

% uvicorn main:data_service_app --reload
```

API documentation:
````
http://127.0.0.1:8000/docs
http://127.0.0.1:8000/redoc
````