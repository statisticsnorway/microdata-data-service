import re
import sys
import uuid
import json
import logging
import datetime
from time import perf_counter_ns
from typing import Callable

import tomlkit
from fastapi import Request
from contextvars import ContextVar

from data_service.config import environment


def _get_project_meta():
    with open("pyproject.toml", encoding="utf-8") as pyproject:
        file_contents = pyproject.read()
    return tomlkit.parse(file_contents)["tool"]["poetry"]


request_start_time: ContextVar[int] = ContextVar("request_start_time")
correlation_id: ContextVar[str] = ContextVar("correlation_id")
method: ContextVar[str] = ContextVar("method")
url: ContextVar[str] = ContextVar("url")
remote_host: ContextVar[str] = ContextVar("remote_host")
response_status: ContextVar[int] = ContextVar("response_status")
response_time_ms: ContextVar[int] = ContextVar("response_time_ms")


class RequestInfoFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Make sure string only contains alphanumeric characters, underscores and/or dashes
        record.correlation_id = re.sub(r"[^\w\-]", "", correlation_id.get(""))
        record.method = method.get("")
        record.url = url.get("")
        record.remote_host = remote_host.get("")
        record.response_status = response_status.get("")
        record.response_time_ms = response_time_ms.get("")
        return True


class MicrodataJSONFormatter(logging.Formatter):
    def __init__(self):
        self.pkg_meta = _get_project_meta()
        self.host = environment.get("DOCKER_HOST_NAME")
        self.command = json.dumps(sys.argv)

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "@timestamp": datetime.datetime.fromtimestamp(
                    record.created,
                    tz=datetime.timezone.utc,
                ).isoformat(),
                "command": self.command,
                "error.stack": record.__dict__.get("exc_info"),
                "host": self.host,
                "message": record.getMessage(),
                "level": record.levelno,
                "levelName": record.levelname,
                "loggerName": record.name,
                "method": record.__dict__.get("method"),
                "responseTime": record.__dict__.get("response_time_ms"),
                "schemaVersion": "v3",
                "serviceName": "data-service",
                "serviceVersion": str(self.pkg_meta["version"]),
                "source_host": record.__dict__.get("remote_host"),
                "statusCode": record.__dict__.get("response_status"),
                "thread": record.threadName,
                "url": record.__dict__.get("url"),
                "xRequestId": record.__dict__.get("correlation_id"),
            }
        )


def setup_logging(app, log_level=logging.INFO):
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next: Callable):
        logger = logging.getLogger()
        logger.setLevel(log_level)

        formatter = MicrodataJSONFormatter()

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        request_info_filter = RequestInfoFilter()
        logger.addFilter(request_info_filter)
        logger.addHandler(stream_handler)

        request_start_time.set(perf_counter_ns())
        corr_id = request.headers.get("X-Request-ID", None)
        if corr_id is None:
            correlation_id.set("data-service-" + str(uuid.uuid1()))
        else:
            correlation_id.set(corr_id)
        method.set(request.method)
        url.set(str(request.url))
        client = request.client
        host = ""
        if client is not None:
            host = client.host
        remote_host.set(host)

        response = await call_next(request)

        response_time = int(
            (perf_counter_ns() - request_start_time.get()) / 1_000_000
        )
        response_time_ms.set(response_time)
        response_status.set(response.status_code)

        response.headers["X-Request-ID"] = correlation_id.get()
        return response
