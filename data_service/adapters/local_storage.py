import json
import logging
import os
from typing import Union

from pyarrow import Table
from pyarrow import dataset, parquet

from data_service.config import environment
from data_service.exceptions import NotFoundException


DATASTORE_DIR = environment.get('DATASTORE_DIR')
DATA_DIR = f'{DATASTORE_DIR}/data'
logger = logging.getLogger(__name__ + '.local_storage')


def _get_parquet_file_path(dataset_name: str, version: str) -> str:
    path_prefix = f'{DATA_DIR}/{dataset_name}'
    if version == '0_0':
        full_path = _get_draft_file_path(path_prefix, dataset_name)
        if full_path is None:
            logger.info(f'No DRAFT for {dataset_name}. Using latest version')
            version = _get_latest_version()
        else:
            return full_path
    file_name = _get_file_name_from_data_versions(
        version, dataset_name
    )
    full_path = f'{path_prefix}/{file_name}'

    if not os.path.exists(full_path):
        logger.error(f'{full_path} does not exist')
        raise NotFoundException(
            f'No file exists for {dataset_name} in version {version}'
        )
    return full_path


def _get_file_name_from_data_versions(version: str, dataset_name: str) -> str:
    data_versions_file = (
        f"{DATASTORE_DIR}/datastore/"
        f"data_versions__{version}.json"
    )
    with open(data_versions_file, encoding="utf-8") as f:
        data_versions = json.load(f)

    if dataset_name not in data_versions:
        raise NotFoundException(
            f"No {dataset_name} in data_versions file "
            f"for version {version}"
        )
    return data_versions[dataset_name]


def _get_draft_file_path(
    path_prefix: str, dataset_name: str
) -> Union[None, str]:
    partitioned_parquet_path = f"{path_prefix}/{dataset_name}__DRAFT"
    parquet_path = f'{partitioned_parquet_path}.parquet'
    if os.path.isfile(parquet_path):
        return parquet_path
    elif os.path.isdir(partitioned_parquet_path):
        return partitioned_parquet_path
    else:
        return None


def _get_latest_version():
    datastore_files = os.listdir(f'{DATASTORE_DIR}/datastore')
    data_versions_files = [
        file for file in datastore_files
        if file.startswith('data_versions')
    ]
    data_versions_files.sort()
    latest_data_versions_file = data_versions_files[-1]
    return (
        latest_data_versions_file.strip('.json').strip('data_versions__')
    )


def _log_parquet_info(parquet_file):
    if os.path.isdir(parquet_file):
        _log_info_partitioned_parquet(parquet_file)
    else:
        _log_parquet_details(parquet_file)


def _log_info_partitioned_parquet(parquet_file):
    for subdir, _, files in os.walk(parquet_file):
        for filename in files:
            filepath = subdir + os.sep + filename
            if filepath.endswith(".parquet"):
                _log_parquet_details(filepath)
                # Log just the first file
                return


def _log_parquet_details(parquet_file):
    logger.info(
        f'Parquet file: {parquet_file} '
        f'Parquet metadata: {parquet.read_metadata(parquet_file)} '
        f'Parquet schema: {parquet.read_schema(parquet_file).to_string()}'
    )


def read_parquet(
    dataset_name: str,
    version: str,
    table_filter: dataset.Expression,
    columns: list[str]
) -> Table:
    """
    Reads and filters a parquet file or partition and returns a
    pyarrow.Table with the requested columns.

    * dataset_name: str - name of dataset
    * version: str - '<MAJOR>_<MINOR>' formatted semantic version
    * table_filter: dataset.Expression - filters applied to the table
    * columns: list[str] - names of the columns to include in the
                           returned table
    """
    parquet_path = _get_parquet_file_path(dataset_name, version)
    _log_parquet_info(parquet_path)
    table = (
        dataset.dataset(parquet_path)
        .to_table(filter=table_filter, columns=columns)
    )
    logger.info(f'Number of rows in result set: {table.num_rows}')
    return table
