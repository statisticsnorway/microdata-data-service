import logging
import os
import sys
import uuid
from typing import Final, Union

import pyarrow.parquet as pq
from fastapi import Depends
from pyarrow import Table

from data_service.adapters.storage.file_adapter import FileAdapter
from data_service.adapters.storage.gcs import GcsBucketAdapter
from data_service.adapters.storage.local import LocalFileAdapter
from data_service.api.query_models import (
    InputTimePeriodQuery, InputTimeQuery, InputFixedQuery
)
from data_service.config import config
from data_service.config.config import get_settings
from data_service.core import filters


class Processor:
    EMPTY_RESULT_TEXT: Final[str] = "empty_result"

    def __init__(self, settings: config.BaseSettings = Depends(get_settings)):
        super().__init__()
        self.log = logging.getLogger(__name__ + '.Processor')
        self.settings = settings

    def process_event_request(self, input_query: InputTimePeriodQuery) -> Union[Table, str]:
        parquet_file = self.__get_parquet_file_path__(input_query)
        if parquet_file is None:
            return f'dataset_{input_query.dataStructureName}_not_found'

        self.__log_parquet_info__(parquet_file)

        data = filters.filter_by_time_period(
            parquet_file, input_query.startDate, input_query.stopDate,
            input_query.population, input_query.includeAttributes
        )
        return data

    def process_status_request(self, input_query: InputTimeQuery) -> Union[Table, str]:
        parquet_file = self.__get_parquet_file_path__(input_query)
        if parquet_file is None:
            return f'dataset_{input_query.dataStructureName}_not_found'

        self.__log_parquet_info__(parquet_file)

        data = filters.filter_by_time(
            parquet_file, input_query.date, input_query.population,
            input_query.includeAttributes
        )
        return data

    def process_fixed_request(self, input_query: InputFixedQuery) -> Union[Table, str]:
        parquet_file = self.__get_parquet_file_path__(input_query)
        if parquet_file is None:
            return f'dataset_{input_query.dataStructureName}_not_found'

        self.__log_parquet_info__(parquet_file)

        data = filters.filter_by_fixed(
            parquet_file, input_query.population,
            input_query.includeAttributes
        )
        return data

    def __log_parquet_info__(self, parquet_file):
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
        self.log.info(
            f'Parquet file: {parquet_file} '
            f'Parquet metadata: {pq.read_metadata(parquet_file)} '
            f'Parquet schema: {pq.read_schema(parquet_file).to_string()}'
        )

    def __log_result_info__(self, data, result_file_path):
        size = sys.getsizeof(data)
        self.log.info(
            'Size of filtered pyarrow table: '
            f'{size} bytes ({size / 1000000} MB)'
        )
        self.log.info(
            'Parquet metadata of result set: '
            f'{pq.read_metadata(result_file_path)}'
        )
        self.log.info(
            'Size of file with result set: '
            f'{os.path.getsize(result_file_path) / 1000000} MB'
        )

    def __get_parquet_file_path__(self, input_query):
        file_service: FileAdapter = self.__get_storage__()
        return file_service.get_parquet_file_path(input_query.dataStructureName, input_query.version)

    def __get_storage__(self):
        if isinstance(self.settings, config.GoogleCloudSettings):
            self.log.info('Using GcsBucketAdapter')
            return GcsBucketAdapter(self.settings)
        else:
            self.log.info('Using LocalFiledapter')
            return LocalFileAdapter(self.settings)

    def write_table(self, data):
        result_filename = f'{str(uuid.uuid4())}.parquet'
        result_file_path = (
            f'{self.settings.RESULTSET_DIR}/{result_filename}'
        )
        pq.write_table(data, result_file_path)
        self.__log_result_info__(data, result_file_path)
        return result_filename


