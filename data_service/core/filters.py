import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow.dataset import Expression

read_table_columns = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]


def filter_by_time_period(parquet_partition_name, start: int, stop: int):
    stop_missing: Expression = ~ds.field("stop_epoch_days").is_valid()
    start_epoch_le_start: Expression = ds.field('start_epoch_days') <= start
    start_epoch_ge_start: Expression = ds.field('start_epoch_days') >= start
    start_epoch_le_stop: Expression = ds.field('start_epoch_days') <= stop
    start_epoch_g_start: Expression = ds.field('start_epoch_days') > start
    stop_epoch_ge_start: Expression = ds.field('stop_epoch_days') >= start
    stop_epoch_le_stop: Expression = ds.field('stop_epoch_days') <= stop

    find_by_time_period_filter = (start_epoch_le_start & stop_missing) | \
                                 (start_epoch_le_start & stop_epoch_ge_start) | \
                                 (start_epoch_ge_start & start_epoch_le_stop) | \
                                 (start_epoch_g_start & stop_epoch_le_stop)
    table = None
    try:
        table = pq.read_table(source=parquet_partition_name, filters=find_by_time_period_filter,
                              columns=read_table_columns)
    except:
        print('Empty resultset')
    return table


def filter_by_time(parquet_partition_name, date: int):
    stop_missing: Expression = ~ds.field("stop_epoch_days").is_valid()
    start_epoch_le_date: Expression = ds.field('start_epoch_days') <= date
    stop_epoch_ge_date: Expression = ds.field('stop_epoch_days') >= date

    find_by_time_filter = (start_epoch_le_date & stop_missing) | \
                          (start_epoch_le_date & stop_epoch_ge_date)
    table = None
    try:
        table = pq.read_table(source=parquet_partition_name, filters=find_by_time_filter,
                              columns=read_table_columns)
    except:
        print('Empty resultset')
    return table


def filter_by_fixed():
    raise Exception('No filtering of fixed datasets! '
                    'A fixed dataset query returns the whole dataset without filtering')
