import logging
import os
from typing import Final

import sys
import uuid

import pyarrow.parquet as pq
from fastapi import Depends

from data_service.adapters.storage.gcs import GcsBucketAdapter
from data_service.adapters.storage.local import LocalFileAdapter
from data_service.api.query_models import InputTimePeriodQuery, InputTimeQuery, InputFixedQuery
from data_service.config import config
from data_service.config.config import get_settings
from data_service.core import filters
from data_service.core.file_adapter import FileAdapter


class Processor:
    EMPTY_RESULT_TEXT: Final[str] = "empty_result"

    def __init__(self, settings: config.BaseSettings = Depends(get_settings)):
        super().__init__()
        self.log = logging.getLogger(__name__ + '.Processor')
        self.settings = settings

    def process_event_request(self, input_query: InputTimePeriodQuery) -> str:
        parquet_file = self.get_parquet_file_path(input_query)
        if parquet_file is None:
            return f'dataset_{input_query.dataStructureName}_not_found'

        self.log_parquet_info(parquet_file)

        data = filters.filter_by_time_period(parquet_file, input_query.startDate, input_query.stopDate,
                                             input_query.population, input_query.include_attributes)

        return self.__write_table__(data)

    def process_status_request(self, input_query: InputTimeQuery) -> str:
        parquet_file = self.get_parquet_file_path(input_query)
        if parquet_file is None:
            return f'dataset_{input_query.dataStructureName}_not_found'

        self.log_parquet_info(parquet_file)

        data = filters.filter_by_time(parquet_file, input_query.date, input_query.population,
                                      input_query.include_attributes)

        return self.__write_table__(data)

    def process_fixed_request(self, input_query: InputFixedQuery) -> str:
        parquet_file = self.get_parquet_file_path(input_query)
        if parquet_file is None:
            return f'dataset_{input_query.dataStructureName}_not_found'

        self.log_parquet_info(parquet_file)

        data = filters.filter_by_fixed(parquet_file, input_query.population, input_query.include_attributes)

        return self.__write_table__(data)

    def log_parquet_info(self, parquet_file):
        if os.path.isdir(parquet_file):
            self.__log_info_partitioned_parquet__(parquet_file)
        else:
            self.__log_parquet_details__(parquet_file)

    def __log_info_partitioned_parquet__(self, parquet_file):
        for subdir, dirs, files in os.walk(parquet_file):
            for filename in files:
                filepath = subdir + os.sep + filename
                if filepath.endswith(".parquet"):
                    self.__log_parquet_details__(filepath)

    def __log_parquet_details__(self, parquet_file):
        self.log.info(f'Parquet file: {parquet_file} '
                      f'Parquet metadata: {pq.read_metadata(parquet_file)} '
                      f'Parquet schema: {pq.read_schema(parquet_file).to_string()}')

    def log_result_info(self, data, result_filename):
        size = sys.getsizeof(data)
        self.log.info(f'Size of filtered pyarrow table: {size} bytes ({size / 1000000} MB)')
        self.log.info(f'Parquet metadata of result set: {pq.read_metadata(result_filename)}')
        self.log.info(f'Size of file with result set: {os.path.getsize(result_filename) / 1000000} MB')

    def get_parquet_file_path(self, input_query):
        file_service: FileAdapter = self.__get_storage__()
        parquet_file = file_service.get_file(path=input_query.dataStructureName)
        return parquet_file

    def __get_storage__(self):
        if isinstance(self.settings, config.GoogleCloudSettings):
            self.log.info('Using GcsBucketAdapter')
            return GcsBucketAdapter(self.settings)
        else:
            self.log.info('Using LocalFiledapter')
            return LocalFileAdapter(self.settings)

    def __write_table__(self, data):
        if data and data.num_rows > 0:
            result_filename = (
                f'{self.settings.FILE_SERVICE_DATASTORE_ROOT_PREFIX}'
                f'/resultset/{str(uuid.uuid4())}.parquet'
            )
            pq.write_table(data, result_filename)
            self.log_result_info(data, result_filename)
            return result_filename
        else:
            return self.EMPTY_RESULT_TEXT
