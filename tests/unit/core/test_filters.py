import unittest
import glob
import pandas
import pyarrow.parquet as pq
from data_service.core.filters import filter_by_time_period
from tests.unit.util.util import convert_csv_to_parquet

# FRA Datastore-API
#
# INSERT INTO `BOSTED__3_2`(part_num, unit_id, value, start, stop, start_type, stop_type, attributes)
# VALUES
# (0, 1000000001, '3', '1985-01-01', '1991-06-30', 20, NULL, NULL),
# (80, 1000000001, '16', '1991-07-01', '1992-03-31', 20, NULL, NULL),
# (130, 1000000001, '3', '1992-04-01', NULL, 20, NULL, NULL),
# (180, 1000000002, '8', '1974-01-01', '1992-01-31', 20, NULL, NULL),
# (230, 1000000002, '8', '1992-02-01', NULL, 20, NULL, NULL),
# (299, 1000000003, '12', '1981-01-01', '1991-01-31', 20, NULL, NULL),
# (450, 1000000003, '2', '1991-02-01', '1991-10-14', 20, NULL, NULL),
# (499, 1000000003, '12', '1991-10-15', NULL, 20, NULL, NULL),
# (999, 1000000004, '2', '1979-01-01', '1991-02-10', 40, NULL, NULL),
# ;
#
#
#
#
# private static final DATUM1 = new Datum(1000000001, '3', LocalDate.of(1985, 1, 1), LocalDate.of(1991, 6, 30))
# private static final DATUM2 = new Datum(1000000002, '8', LocalDate.of(1974, 1, 1), LocalDate.of(1992, 1, 31))
# private static final DATUM3 = new Datum(1000000003, '12', LocalDate.of(1981, 1, 1), LocalDate.of(1991, 1, 31))
# private static final DATUM4 = new Datum(1000000004, '2', LocalDate.of(1979, 1, 1), LocalDate.of(1991, 2, 10))
# private static final DATUM5 = new Datum(1000000003, '2', LocalDate.of(1991, 2, 1), LocalDate.of(1991, 10, 14))
# private static final DATUM6 = new Datum(1000000003, '12', LocalDate.of(1991, 10, 15), null)
# private static final DATUM7 = new Datum(1000000001, '16', LocalDate.of(1991, 7, 1), LocalDate.of(1992, 3, 31))
# private static final DATUM8 = new Datum(1000000002, '8', LocalDate.of(1992, 2, 1), null)
# private static final DATUM9 = new Datum(1000000001, '3', LocalDate.of(1992, 4, 1), null)
#
#
#
# EventQuery query = createEventQuery('1991-01-01', '1991-12-31', 'BOSTED')
#
# Collection<Datum> expectedDatums = [DATUM1, DATUM7, DATUM2, DATUM3, DATUM5, DATUM6, DATUM4]
#


class TestFilters(unittest.TestCase):

    def test_filter_by_time_period(self):
        # ../../tests/resources/unit_test_data/TEST_BOSTED__3_2.csv
        # Dette på grunn av OSError: [Errno 30] Read-only file system: '/resources'
        csv_file = 'TEST_BOSTED__3_2.csv'
        parquet_path = 'TEST_BOSTED__3_2'


        convert_csv_to_parquet(csv_file, parquet_path, False)
        parquet_file = glob.glob("TEST_BOSTED__3_2/*.parquet")[0]

        # parquet_file = 'TEST_BOSTED__3_2/1a5324c3d6844e1788f6719b934dc069.parquet'
        print(parquet_file)

        result_table = filter_by_time_period(parquet_file, 7670, 8034)

        if (result_table is not None):
            pq.write_table(result_table, "TEST_BOSTED__3_2/result.parquet")

            parquet_table = pq.read_table(source="TEST_BOSTED__3_2/result.parquet")

            print('Reultat dataset')
            print('-----------------------------------------------------------')
            print(parquet_table.to_pandas().head(10))
            print('-----------------------------------------------------------')

