FROM python:slim

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

CMD [ "uvicorn", "--host=0.0.0.0",  "data_service.main:data_service_app"]

