import logging
from typing import Union

from pyarrow import Table

from data_service.domain import filters
from data_service.adapters import local_storage
from data_service.api.query_models import (
    InputTimePeriodQuery, InputTimeQuery, InputFixedQuery
)


EMPTY_RESULT_TEXT = "empty_result"
ALL_COLUMNS = [
    'unit_id', 'value', 'start_epoch_days', 'stop_epoch_days'
]
logger = logging.getLogger(__name__ + '.data')


def process_event_request(
    input_query: InputTimePeriodQuery
) -> Union[Table, str]:
    table_filter = filters.generate_time_period_filter(
        input_query.startDate, input_query.stopDate, input_query.population
    )
    columns = (
        ALL_COLUMNS if input_query.includeAttributes
        else ALL_COLUMNS[:2]
    )
    return local_storage.read_parquet(
        input_query.dataStructureName,
        input_query.get_file_version(),
        table_filter,
        columns
    )


def process_status_request(
    input_query: InputTimeQuery
) -> Union[Table, str]:
    table_filter = filters.generate_time_filter(
        input_query.date, input_query.population
    )
    columns = (
        ALL_COLUMNS if input_query.includeAttributes
        else ALL_COLUMNS[:2]
    )
    return local_storage.read_parquet(
        input_query.dataStructureName,
        input_query.get_file_version(),
        table_filter,
        columns
    )


def process_fixed_request(
    input_query: InputFixedQuery
) -> Union[Table, str]:
    table_filter = filters.generate_population_filter(
        input_query.population
    )
    columns = (
        ALL_COLUMNS if input_query.includeAttributes
        else ALL_COLUMNS[:2]
    )
    return local_storage.read_parquet(
        input_query.dataStructureName,
        input_query.get_file_version(),
        table_filter,
        columns
    )
