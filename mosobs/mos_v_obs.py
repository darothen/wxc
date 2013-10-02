import sys, os
import numpy as np
import datetime
import itertools
from operator import mul

from pandas import *

from util import mos, obs

from pylab import *
ion()

def mos_maxmin_fcsts(station, interval, model):
    '''
    Aggregate a bunch of forecast data in order to determine
    '''
    start_date, end_date = interval
    day_count = (end_date - start_date).days + 1

    path_to_mos_data = os.path.join(os.getcwd(), "data_arch", station, model)

    all_fcsts = []
    dates = [start_date + datetime.timedelta(n) for n in range(day_count)]
    for date in dates:
        print date
        y, m, d = date.year, date.month, date.day
        day1 = date + datetime.timedelta(1)
        day2 = date + datetime.timedelta(2)

        day1_maxes, day1_mins, day2_maxes, day2_mins = [], [], [], []
        fcsts = {}

        fcst_hours = [0, 6, 12, 18] if model == "GFS" else [0, 12]

        for h in fcst_hours:
            fn = "%s.%02d%02d%4d.%s.%02dZ" % (station, m, d, y, mos.full_model_name[model], h)
            f = open(os.path.join(path_to_mos_data, fn), "r")

            proc_mos = mos.process_MOS(f.readlines())
            maxmins = proc_mos.maxmins

            mm_day1 = maxmins[day1]
            day1_max, day1_min = mm_day1['max'], mm_day1['min']

            mm_day2 = maxmins[day2]
            day2_max, day2_min = mm_day2['max'], mm_day2['min']

            day1_maxes.append(day1_max)
            day2_maxes.append(day2_max)
            day1_mins.append(day1_min)
            day2_mins.append(day2_min)

        all_fcsts.append([ [day1_maxes, day2_maxes], [day1_mins, day2_mins] ])

    all_fcsts = np.array(all_fcsts)
    length = len(all_fcsts)
    ncols = reduce(mul, all_fcsts.shape[1:])

    all_fcsts = np.reshape(all_fcsts, [length, ncols])
    print all_fcsts.shape

    tuples = list(itertools.product(["Tmax", "Tmin"], ["day1", "day2"], ["%02dZ" % h for h in fcst_hours]))
    cols = MultiIndex.from_tuples(tuples, names=['field', 'fcst day', 'model run'])

    print cols
    return DataFrame(all_fcsts, index=dates, columns=cols)

if __name__ == "__main__":

    station_name = "KAUS"

    ## Observations
    variable_data, core = obs.parse_observations("data_arch/%s/obs.csv" % station_name)

    start_date = datetime.datetime(2009, 1, 1)
    end_date = datetime.datetime(2009, 12, 31)
    interval = [start_date, end_date]

    ## The timestamp in these DataFrames is from the perspective of the forecast date,
    ## so "Day1" on timestamp "12/01/2009" is the forecast validating on "12/02/2009"
    gfs_fcsts = mos_maxmin_fcsts(station_name, interval, "GFS")
    gfs_Tmax, gfs_Tmin = gfs_fcsts['Tmax'], gfs_fcsts['Tmin']
    gfs_Tmax_d1, gfs_Tmax_d2 = gfs_Tmax['day1'], gfs_Tmax['day2']
    gfs_Tmin_d1, gfs_Tmin_d2 = gfs_Tmin['day1'], gfs_Tmin['day2']

    fcst_reindex = lambda df, days: df.set_index(df.index - datetime.timedelta(days), inplace=True)
    fcst_reindex(gfs_Tmax_d1, 1); fcst_reindex(gfs_Tmin_d1, 1);
    fcst_reindex(gfs_Tmax_d2, 2); fcst_reindex(gfs_Tmin_d2, 2);

    nam_fcsts = mos_maxmin_fcsts(station_name, interval, "NAM")

    nam_Tmax, nam_Tmin = nam_fcsts['Tmax'], nam_fcsts['Tmin']
    nam_Tmax_d1, nam_Tmax_d2 = nam_Tmax['day1'], nam_Tmax['day2']
    nam_Tmin_d1, nam_Tmin_d2 = nam_Tmin['day1'], nam_Tmin['day2']

    fcst_reindex(nam_Tmax_d1, 1); fcst_reindex(nam_Tmin_d1, 1);
    fcst_reindex(nam_Tmax_d2, 2); fcst_reindex(nam_Tmin_d2, 2);

    ## Try appending all the d1/d2 together for nam/gfs into one very wide spreadsheet of fcsts
    # 1) rename columns
    gfs_Tmax_d1.columns = ["GFS day1 %s" % c for c in gfs_Tmax_d1.columns]
    gfs_Tmax_d2.columns = ["GFS day2 %s" % c for c in gfs_Tmax_d2.columns]
    nam_Tmax_d1.columns = ["NAM day1 %s" % c for c in nam_Tmax_d1.columns]
    nam_Tmax_d2.columns = ["NAM day2 %s" % c for c in nam_Tmax_d2.columns]

    Tmax = gfs_Tmax_d1.append(gfs_Tmax_d2)
    Tmax = Tmax.append(nam_Tmax_d1)
    Tmax = Tmax.append(nam_Tmax_d2)

