import os
import pyarrow.parquet as pq
import sys
import uuid

from data_service.adapters.storage.gcs import GcsBucketAdapter
from data_service.adapters.storage.local import LocalFileAdapter
from data_service.api.query_models import InputQuery
from data_service.config import config
from data_service.config.logging import get_logger
from data_service.core.file_adapter import FileAdapter

log = get_logger(__name__)


def process(input_query: InputQuery, settings: config.Settings) -> str:
    file_service: FileAdapter = get_storage(settings)
    parquet_file = file_service.get_file(path=input_query.dataStructureName)

    log.info(f'Parquet metadata: {pq.read_metadata(parquet_file)}')
    log.info(f'Parquet schema: {pq.read_schema(parquet_file).to_string()}')

    #TODO data = pq.read_table(source=parquet_file, filters=[('start', '>=', input_query.startDate), ('stop', '<=', input_query.stopDate)])
    data = pq.read_table(source=parquet_file)
    size = sys.getsizeof(data)
    log.info(f'Size of filtered pyarrow table: {size} bytes ({size/1000000} MB)')

    result_filename = str(uuid.uuid4()) + '.parquet'
    # log.info(f'Resultset: {data.to_pandas().head(50)}')
    pq.write_table(data, result_filename)
    log.info(f'Parquet metadata of result set: {pq.read_metadata(result_filename)}')
    log.info(f'Size of file with result set: {os.path.getsize(result_filename)/1000000} MB')

    return result_filename


def get_storage(settings: config.Settings):
    if settings.STORAGE_ADAPTER == 'GCS':
        log.info('Using GcsBucketAdapter')
        return GcsBucketAdapter(settings)
    else:
        log.info('Using LocalFiledapter')
        return LocalFileAdapter(settings)
