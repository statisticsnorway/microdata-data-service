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

    stop_epoch_e_stop: Expression = ds.field('stop_epoch_days') == 8065

    find_by_time_period_filter = [  # ingen treff
        [('start_epoch_days', '<=', start), stop_missing],
        [('start_epoch_days', '<=', start), ('stop_epoch_days', ">=", start)],
        [('start_epoch_days', '>=', start), ('start_epoch_days', '<=', stop)],
        [('start_epoch_days', '>', start), ('stop_epoch_days', "<=", stop)]
    ]

    find_by_time_period_filter_2 = [  # ingen treff
        [start_epoch_le_start, stop_missing],
        [start_epoch_le_start, stop_epoch_ge_start],
        [start_epoch_ge_start, start_epoch_le_stop],
        [start_epoch_g_start, stop_epoch_le_stop]
    ]

    # ingen treff
    find_by_time_period_filter_3 = \
        [(start_epoch_le_start and stop_missing) or
         (start_epoch_le_start and stop_epoch_ge_start) or
         (start_epoch_ge_start and start_epoch_le_stop) or
         (start_epoch_g_start and stop_epoch_le_stop)]

    # riktig : 4 treff
    #    find_by_time_period_filter_4 = [('start_epoch_days', '<=', start)]
    # feil : ingen treff
    #    find_by_time_period_filter_4 = ('start_epoch_days', '<=', start)

    # riktig : 3 treff
    find_by_time_period_filter_5 = stop_missing

    # find_by_time_period_filter = (start_epoch_le_start and stop_missing) #or (start_epoch_le_start and stop_epoch_ge_start)

    gir_feil_treff = \
        ((start_epoch_le_start and stop_missing) or (start_epoch_le_start and stop_epoch_ge_start))

    find_by_time_period_halv_filter_1 = (start_epoch_ge_start and stop_missing)

    # SE HER!! Ulik resultat avhengig av [ ]
    gir_ingen_treff = \
        [(start_epoch_le_start and stop_epoch_ge_start)]

    gir_feil_treff_2 = (start_epoch_le_start and stop_epoch_ge_start)
    # 1000000003     2              7701             7956

    gir_også_feil_treff = (start_epoch_ge_start and stop_epoch_le_stop)

    print(start)
    print(stop)

    table = None
    try:
        table = pq.read_table(source=parquet_file, filters=gir_feil_treff_2)
    except:
        print('Ingen treff')
    return table
