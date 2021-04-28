import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow.dataset import Expression

columns_including_attributes = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]

columns_excluding_attributes = ["unit_id", "value"]


def filter_by_time_period(parquet_partition_name: str, start: int, stop: int, incl_attributes=False):
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

    table = do_filter(find_by_time_period_filter, incl_attributes, parquet_partition_name)
    return table


def filter_by_time(parquet_partition_name: str, date: int, incl_attributes=False):
    stop_missing: Expression = ~ds.field("stop_epoch_days").is_valid()
    start_epoch_le_date: Expression = ds.field('start_epoch_days') <= date
    stop_epoch_ge_date: Expression = ds.field('stop_epoch_days') >= date

    find_by_time_filter = (start_epoch_le_date & stop_missing) | \
                          (start_epoch_le_date & stop_epoch_ge_date)

    table = do_filter(find_by_time_filter, incl_attributes, parquet_partition_name)
    return table


def do_filter(filter: Expression, incl_attributes: bool, parquet_partition_name: str):
    table = None
    try:
        if incl_attributes:
            table = pq.read_table(source=parquet_partition_name, filters=filter,
                                  columns=columns_including_attributes)
        else:
            table = pq.read_table(source=parquet_partition_name, filters=filter,
                                  columns=columns_excluding_attributes)
    except:
        print('Empty resultset')
    return table


def filter_by_fixed():
    raise Exception('No filtering of fixed datasets! '
                    'A fixed dataset query returns the whole dataset without filtering')
