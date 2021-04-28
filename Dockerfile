FROM python:slim

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

#the output is sent straight to terminal without being first buffered
ENV PYTHONUNBUFFERED 1

CMD [ "uvicorn", "--host=0.0.0.0",  "application:data_service_app"]

