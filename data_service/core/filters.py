import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow.dataset import Expression


def filter_by_time_period(parquet_file, start: int, stop: int):

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
        table = pq.read_table(source=parquet_file, filters=find_by_time_period_filter)
    except:
        print('Empty resultset')
    return table
