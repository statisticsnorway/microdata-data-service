import logging
import os
import sys
import uuid

import pyarrow.parquet as pq

from data_service.adapters.storage.gcs import GcsBucketAdapter
from data_service.adapters.storage.local import LocalFileAdapter
from data_service.api.query_models import InputTimePeriodQuery, InputTimeQuery, InputFixedQuery
from data_service.config import config
from data_service.core import filters
from data_service.core.file_adapter import FileAdapter


def process_event_request(input_query: InputTimePeriodQuery, settings: config.Settings) -> str:
    log = logging.getLogger(__name__)
    parquet_file = get_parquet(input_query, settings)

    log_parquet_info(log, parquet_file)

    data = filters.filter_by_time_period(parquet_file, input_query.startDate, input_query.stopDate,
                                         input_query.population, input_query.include_attributes)

    result_filename = "empty_result"
    if data is not None:
        result_filename = str(uuid.uuid4()) + '.parquet'
        pq.write_table(data, result_filename)
        log_result_info(data, log, result_filename)

    return result_filename


def process_status_request(input_query: InputTimeQuery, settings: config.Settings) -> str:
    log = logging.getLogger(__name__)
    parquet_file = get_parquet(input_query, settings)

    log_parquet_info(log, parquet_file)

    data = filters.filter_by_time(parquet_file, input_query.date, input_query.population,
                                  input_query.include_attributes)

    result_filename = "empty_result"
    if data is not None:
        result_filename = str(uuid.uuid4()) + '.parquet'
        pq.write_table(data, result_filename)
        log_result_info(data, log, result_filename)

    return result_filename


def process_fixed_request(input_query: InputFixedQuery, settings: config.Settings) -> str:
    log = logging.getLogger(__name__)
    parquet_file = get_parquet(input_query, settings)

    log_parquet_info(log, parquet_file)

    data = filters.filter_by_fixed(parquet_file, input_query.population, input_query.include_attributes)

    result_filename = "empty_result"
    if data is not None:
        result_filename = str(uuid.uuid4()) + '.parquet'
        pq.write_table(data, result_filename)
        log_result_info(data, log, result_filename)

    return result_filename


def log_parquet_info(log, parquet_file):
    log.info(f'Parquet metadata: {pq.read_metadata(parquet_file)}')
    log.info(f'Parquet schema: {pq.read_schema(parquet_file).to_string()}')


def log_result_info(data, log, result_filename):
    size = sys.getsizeof(data)
    log.info(f'Size of filtered pyarrow table: {size} bytes ({size / 1000000} MB)')
    log.info(f'Parquet metadata of result set: {pq.read_metadata(result_filename)}')
    log.info(f'Size of file with result set: {os.path.getsize(result_filename) / 1000000} MB')


def get_parquet(input_query, settings):
    file_service: FileAdapter = get_storage(settings)
    parquet_file = file_service.get_file(path=input_query.dataStructureName)
    return parquet_file


def get_storage(settings: config.Settings):
    log = logging.getLogger(__name__)

    if settings.STORAGE_ADAPTER == 'GCS':
        log.info('Using GcsBucketAdapter')
        return GcsBucketAdapter(settings)
    else:
        log.info('Using LocalFiledapter')
        return LocalFileAdapter(settings)
