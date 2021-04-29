import logging
import os
import shutil
from pathlib import Path

import pandas
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq


def convert_csv_to_parquet(csv_file: str, parquet_partition_name: str):
    log = logging.getLogger(__name__)
    log.info('Start convert csv to parquet')

    log.info(f'Abs path of csv file: {os.path.abspath(csv_file)}')

    #Remove old file
    if Path(parquet_partition_name).is_dir():
        shutil.rmtree(parquet_partition_name)

    # ReadOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ReadOptions.html#pyarrow.csv.ReadOptions
    csv_read_options = pv.ReadOptions(
        skip_rows=0,
        encoding="utf8",
        column_names=["unit_id", "value", "start", "stop", "start_year", "start_epoch_days", "stop_epoch_days"])

    # ParseOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ParseOptions.html#pyarrow.csv.ParseOptions
    csv_parse_options = pv.ParseOptions(delimiter=';')

    # Types: https://arrow.apache.org/docs/python/api/datatypes.html
    # TODO nullable parameter does not work as expected!
    data_schema = pa.schema([
        pa.field(name='start_year', type=pa.string(), nullable=True),
        pa.field(name='unit_id', type=pa.uint64(), nullable=False),
        pa.field(name='value', type=pa.string(), nullable=False),
        pa.field(name='start_epoch_days', type=pa.int16(), nullable=True),
        pa.field(name='stop_epoch_days', type=pa.int16(), nullable=True),
    ])

    # ConvertOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html#pyarrow.csv.ConvertOptions
    csv_convert_options = pv.ConvertOptions(column_types=data_schema)
                                            # include_columns=["unit_id", "value", "start_year", "start_epoch_days", "stop_epoch_days"])

    # read_csv: https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html#pyarrow.csv.read_csv
    table = pv.read_csv(input_file=csv_file, read_options=csv_read_options, parse_options=csv_parse_options,
                        convert_options=csv_convert_options)

    log.info(f'Bytes: {table.nbytes}')
    log.info(f'Rows: {table.num_rows}')
    log.info(f'Schema: {table.schema}')
    log.info(f'Column names: {table.column_names}')
    pandas.set_option('max_columns', None)  # print all columns
    log.info(table.to_pandas().head(10))

    # write with partitions
    pq.write_to_dataset(table,
                        root_path=parquet_partition_name,
                        partition_cols=['start_year'])

    log.info('End convert csv to parquet')
