import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow.dataset import Expression


def filter_by_time_period(parquet_file, start, stop):
    stop_missing: Expression = ~ds.field("stop_epoch_days").is_valid()

    # find_by_time_period_filter = [
    #     [('start_epoch_days', '<=', start), stop_missing],
    #     [('start_epoch_days', '<=', start), ('stop_epoch_days', ">=", start)],
    #     [('start_epoch_days', '>=', start), ('start_epoch_days', '<=', stop)],
    #     [('start_epoch_days', '>', start), ('stop_epoch_days', "<=", stop)]
    # ]

#    find_by_time_period_filter = [stop_missing]

    #find_by_time_period_filter = [('start_epoch_days', '<=', start)]

    stop_missing: Expression = ~ds.field("stop_epoch_days").is_valid()

    start_epoch_le_start: Expression = ds.field('start_epoch_days') <= start

    start_epoch_ge_start: Expression = ds.field('start_epoch_days') >= start

    start_epoch_le_stop: Expression = ds.field('start_epoch_days') <= stop

    start_epoch_g_start: Expression = ds.field('start_epoch_days') > start

    stop_epoch_ge_start: Expression = ds.field('stop_epoch_days') >= start

    stop_epoch_le_stop: Expression = ds.field('stop_epoch_days') <= stop

    stop_epoch_e_stop: Expression = ds.field('stop_epoch_days') == 8065

    find_by_time_period_filter = \
        [(start_epoch_le_start and stop_missing) or
         (start_epoch_le_start and stop_epoch_ge_start) or
         (start_epoch_ge_start and start_epoch_le_stop) or
         (start_epoch_g_start and stop_epoch_le_stop)]

    gir_feil_treff = \
        ((start_epoch_le_start and stop_missing) or (start_epoch_le_start and stop_epoch_ge_start))

    find_by_time_period_halv_filter_1 = \
        [(start_epoch_le_start and stop_missing)]

    gir_ingen_treff = \
        [(start_epoch_le_start and stop_epoch_ge_start)]

    gir_feil_treff_2 = (start_epoch_le_start and stop_epoch_ge_start)
    # 1000000003     2              7701             7956

    gir_riktig_treff = (start_epoch_ge_start and stop_epoch_le_stop)


    print(start)
    print(stop)

    table = None
    try:
        table = pq.read_table(source=parquet_file, filters=gir_riktig_treff)
    except:
        print('Ingen treff')
    return table
