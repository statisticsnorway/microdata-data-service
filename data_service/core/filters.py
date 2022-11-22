import logging

import pyarrow.dataset as ds
from pyarrow import Table

logger = logging.getLogger()

columns_including_attributes = [
    "unit_id", "value", "start_epoch_days", "stop_epoch_days"
]
columns_excluding_attributes = [
    "unit_id", "value"
]


def filter_by_time_period(parquet_partition_name: str, start: int, stop: int,
                          population_filter: list = None,
                          incl_attributes=False) -> Table:
    stop_missing = ~ds.field("stop_epoch_days").is_valid()
    start_epoch_le_start = ds.field('start_epoch_days') <= start
    start_epoch_ge_start = ds.field('start_epoch_days') >= start
    start_epoch_le_stop = ds.field('start_epoch_days') <= stop
    start_epoch_g_start = ds.field('start_epoch_days') > start
    stop_epoch_ge_start = ds.field('stop_epoch_days') >= start
    stop_epoch_le_stop = ds.field('stop_epoch_days') <= stop

    find_by_time_period_filter = (
        (start_epoch_le_start & stop_missing) |
        (start_epoch_le_start & stop_epoch_ge_start) |
        (start_epoch_ge_start & start_epoch_le_stop) |
        (start_epoch_g_start & stop_epoch_le_stop)
    )
    if population_filter:
        population = ds.field("unit_id").isin(population_filter)
        find_by_time_period_filter = population & find_by_time_period_filter

    table = do_filter(
        find_by_time_period_filter, incl_attributes, parquet_partition_name
    )
    return table


def filter_by_time(parquet_partition_name: str, date: int,
                   population_filter: list = None,
                   incl_attributes=False) -> Table:
    stop_missing = ~ds.field("stop_epoch_days").is_valid()
    start_epoch_le_date = ds.field('start_epoch_days') <= date
    stop_epoch_ge_date = ds.field('stop_epoch_days') >= date

    find_by_time_filter = (
        (start_epoch_le_date & stop_missing) |
        (start_epoch_le_date & stop_epoch_ge_date)
    )
    if population_filter:
        population = ds.field("unit_id").isin(population_filter)
        find_by_time_filter = population & find_by_time_filter

    table = do_filter(
        find_by_time_filter, incl_attributes, parquet_partition_name
    )
    return table


def filter_by_fixed(parquet_partition_name: str,
                    population_filter: list = None,
                    incl_attributes=False) -> Table:
    if population_filter:
        fixed_filter = ds.field("unit_id").isin(population_filter)
        table = do_filter(
            fixed_filter, incl_attributes, parquet_partition_name
        )
    else:
        table = do_filter(None, incl_attributes, parquet_partition_name)
    return table


def do_filter(
    table_filter,
    incl_attributes: bool,
    parquet_partition_name: str
) -> Table:
    if incl_attributes:
        my_dataset = ds.dataset(parquet_partition_name)
        table = my_dataset.to_table(
            filter=table_filter, columns=columns_including_attributes)
    else:
        my_dataset = ds.dataset(parquet_partition_name)
        table = my_dataset.to_table(
            filter=table_filter, columns=columns_excluding_attributes)

    if table.num_rows == 0:
        logger.info("Empty result set")

    return table
