import pyarrow.dataset
import pyarrow.parquet as pq
from pyarrow.dataset import Expression


def filter_by_time_period(parquet_file, start, stop):
    stop_missing: Expression = ~pyarrow.dataset.field("stop_unix_days").is_valid()
    find_by_time_period_filter = [
        [('start_unix_days', '<=', start), stop_missing],
        [('start_unix_days', '<=', start), ('stop_unix_days', ">=", start)],
        [('start_unix_days', '>=', start), ('start_unix_days', '<=', stop)],
        [('start_unix_days', '>', start), ('stop_unix_days', "<=", stop)]
    ]
    data = pq.read_table(source=parquet_file, filters=find_by_time_period_filter)
    return data
