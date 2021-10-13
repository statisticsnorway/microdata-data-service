import os
import shutil
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import jwt


def convert_csv_to_parquet(csv_file: str, parquet_dir: str, partitioned: bool):
    print("Start ", datetime.now())

    print(csv_file)
    print(parquet_dir)

    print("Abs path of csv file: " + os.path.abspath(csv_file))

    #Remove old partitions
    if partitioned:
        if Path(parquet_dir).is_dir():
            shutil.rmtree(parquet_dir)

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
                                    #include_columns=["start_year", "unit_id", "value", "start_epoch_days", "stop_epoch_days"])

    # read_csv: https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html#pyarrow.csv.read_csv
    table = pv.read_csv(input_file=csv_file, read_options=csv_read_options, parse_options=csv_parse_options,
                        convert_options=csv_convert_options)

    # print('Bytes: ' + str(table.nbytes))
    # print('Rows: ' + str(table.num_rows))
    # print('Schema: ' + str(table.schema))
    # print('Column names: ' + str(table.column_names))
    # pandas.set_option('max_columns', None)  # print all columns
    # print(table.to_pandas().head(10))

    # write with partitions

    if partitioned:
        pq.write_to_dataset(table, root_path=parquet_dir, partition_cols=['start_year'])
    else:
        pq.write_to_dataset(table, root_path=parquet_dir)

    print("End ", datetime.now())


def generate_RSA_key_pairs():
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    public_key = private_key.public_key()

    serialized_private_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    )

    serialized_public_key = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    print(serialized_public_key)
    print(serialized_private_key)
    return serialized_private_key, serialized_public_key


def encode_jwt_payload(payload, private_key):
    return jwt.encode(payload, private_key, algorithm="RS256")
