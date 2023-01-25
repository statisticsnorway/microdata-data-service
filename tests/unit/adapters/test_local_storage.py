# pylint: disable=protected-access
import os

from pytest import MonkeyPatch

from data_service.adapters import local_storage
from data_service.domain import filters


TEST_DIR = 'tests/resources/datastore_unit_test/data'
TEST_PERSON_INCOME_PATH = f'{TEST_DIR}/TEST_PERSON_INCOME/TEST_PERSON_INCOME'
TEST_PERSON_INCOME_PATH_1_0 = f'{TEST_PERSON_INCOME_PATH}__1_0.parquet'
TEST_PERSON_INCOME_PATH_DRAFT = f'{TEST_PERSON_INCOME_PATH}__DRAFT.parquet'
TEST_STUDIEPOENG_PATH_1_0 = (
    f'{TEST_DIR}/TEST_STUDIEPOENG/TEST_STUDIEPOENG__1_0'
)
ALL_COLUMNS = [
    'unit_id', 'value', 'start_epoch_days', 'stop_epoch_days'
]


def test_get_file_path():
    assert TEST_PERSON_INCOME_PATH_1_0 == (
        local_storage._get_parquet_file_path(
            'TEST_PERSON_INCOME', '1_0'
        )
     )


def test_get_file_path_draft():
    assert TEST_PERSON_INCOME_PATH_DRAFT == (
        local_storage._get_parquet_file_path(
            'TEST_PERSON_INCOME', '0_0'
        )
     )


def test_get_latest_in_draft_version():
    assert TEST_STUDIEPOENG_PATH_1_0 == (
        local_storage._get_parquet_file_path(
            'TEST_STUDIEPOENG', '0_0'
        )
    )


def test_get_latest_version(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        os, 'listdir', lambda _: [
            'data_versions__10_0.json',
            'data_versions__8_999.json',
            'data_versions__2_0.json'
        ]
    )
    assert '10_0' == (
        local_storage._get_latest_version()
    )
    monkeypatch.setattr(
        os, 'listdir', lambda _: [
            'data_versions__8988_321.json',
            'data_versions__9000_0.json',
            'data_versions__2_0.json'
        ]
    )
    assert '9000_0' == (
        local_storage._get_latest_version()
    )


def test_get_partitioned_file_path():
    assert TEST_STUDIEPOENG_PATH_1_0 == (
        local_storage._get_parquet_file_path(
            'TEST_STUDIEPOENG', '1_0'
        )
    )


def test_read_parquet_no_filter():
    expected_unit_ids = [
        11111111864482,
        11111112296273,
        11111113785911,
        11111113735577,
        11111111454434,
        11111111190644,
        11111113331169,
        11111111923572,
        11111112261125
    ]
    expected_values = [
        '21529182',
        '12687840',
        '16354872',
        '12982099',
        '19330053',
        '11331198',
        '4166169',
        '7394257',
        '6926636'
    ]
    result = local_storage.read_parquet(
        'TEST_PERSON_INCOME', '1_0', None, ALL_COLUMNS
    )
    result_dict = result.to_pydict()
    assert result_dict['unit_id'] == expected_unit_ids
    assert result_dict['value'] == expected_values
    assert (
        len(result_dict['unit_id'])
        == len(result_dict['value'])
        == len(result_dict['start_epoch_days'])
        == len(result_dict['stop_epoch_days'])
    )
    result = local_storage.read_parquet(
        'TEST_PERSON_INCOME', '1_0', None, ALL_COLUMNS[:2]
    )
    result_dict = result.to_pydict()
    assert result_dict['unit_id'] == expected_unit_ids
    assert result_dict['value'] == expected_values
    assert len(result_dict.keys()) == 2


def test_read_parquet_fixed():
    expected_unit_ids = [
        11111111864482,
        11111112296273,
        11111113785911
    ]
    expected_values = [
        '21529182',
        '12687840',
        '16354872'
    ]
    table_filter = filters.generate_population_filter(expected_unit_ids)
    result = local_storage.read_parquet(
        'TEST_PERSON_INCOME', '1_0', table_filter, ALL_COLUMNS
    )
    result_dict = result.to_pydict()
    assert result_dict['unit_id'] == expected_unit_ids
    assert result_dict['value'] == expected_values
    assert len(result_dict.keys()) == 4

    result = local_storage.read_parquet(
        'TEST_PERSON_INCOME', '1_0', table_filter, ALL_COLUMNS[:2]
    )
    result_dict = result.to_pydict()
    assert result_dict['unit_id'] == expected_unit_ids
    assert result_dict['value'] == expected_values
    assert len(result_dict.keys()) == 2


def test_read_parquet_time_period():
    expected_unit_ids = [11111113735577, 11111111190644]
    expected_values = ['12982099', '11331198']
    table_filter = filters.generate_time_period_filter(3000, 10000)
    result = local_storage.read_parquet(
        'TEST_PERSON_INCOME', '1_0', table_filter, ALL_COLUMNS
    )
    result_dict = result.to_pydict()
    assert result_dict['unit_id'] == expected_unit_ids
    assert result_dict['value'] == expected_values
    epoch_days = (
        result_dict['start_epoch_days'] + result_dict['stop_epoch_days']
    )
    for epoch_day in epoch_days:
        assert epoch_day <= 10000 and epoch_day >= 3000


def test_read_parquet_time_period_with_pop_filter():
    expected_unit_ids = [11111113735577]
    expected_values = ['12982099']
    table_filter = filters.generate_time_period_filter(
        3000, 10000, expected_unit_ids
    )
    result = local_storage.read_parquet(
        'TEST_PERSON_INCOME', '1_0', table_filter, ALL_COLUMNS
    )
    result_dict = result.to_pydict()
    assert result_dict['unit_id'] == expected_unit_ids
    assert result_dict['value'] == expected_values
    epoch_days = (
        result_dict['start_epoch_days'] + result_dict['stop_epoch_days']
    )
    for epoch_day in epoch_days:
        assert epoch_day <= 10000 and epoch_day >= 3000


def test_read_parquet_time():
    expected_unit_ids = [11111111864482, 11111112296273]
    expected_values = ['21529182', '12687840']
    table_filter = filters.generate_time_filter(17167)
    result = local_storage.read_parquet(
        'TEST_PERSON_INCOME', '1_0', table_filter, ALL_COLUMNS
    )
    result_dict = result.to_pydict()
    assert result_dict['unit_id'] == expected_unit_ids
    assert result_dict['value'] == expected_values
    for epoch_day in result_dict['start_epoch_days']:
        assert epoch_day <= 17167
    for epoch_day in result_dict['stop_epoch_days']:
        assert epoch_day >= 17167


def test_read_parquet_time_with_pop_filter():
    expected_unit_ids = [11111111864482]
    expected_values = ['21529182']
    table_filter = filters.generate_time_filter(17167, expected_unit_ids)
    result = local_storage.read_parquet(
        'TEST_PERSON_INCOME', '1_0', table_filter, ALL_COLUMNS
    )
    result_dict = result.to_pydict()
    assert result_dict['unit_id'] == expected_unit_ids
    assert result_dict['value'] == expected_values
    for epoch_day in result_dict['start_epoch_days']:
        assert epoch_day <= 17167
    for epoch_day in result_dict['stop_epoch_days']:
        assert epoch_day >= 17167
