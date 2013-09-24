"""
This is a sample script which takes the 18Z GFS and 12Z NAM MOS forecasts and computes several
predicted quantities:

1) Max
2) Min
3) Rain (B)
4) Snow (B) (not implemented yet!)
5) Wind Direction (C)
6) Average Wind Speed

B = Boolean, C = Categorized

The data is then aggregated and a Random Forest is generated to predict whether the Max/Min temperature
is forecast to be too high or too low.
"""

import sys, os
import numpy as np
import datetime
import itertools
from operator import mul

from pandas import *

from util import mos, obs

from pylab import *
ion()


from itertools import cycle
def plot_2D(data, target, target_names):
    colors = cycle('rgbcmykw')
    target_ids = range(len(target_names))
    figure(1)
    clf()
    for i, c, label in zip(target_ids, colors, target_names):
        scatter(data[target == i, 0], data[target == i, 1],
                c=c, label=label)
    legend()
    show()

def plot_3D(data, target, target_names):
    from mpl_toolkits.mplot3d import Axes3D
    colors = cycle('rgbcmykw')
    target_ids = range(len(target_names))
    fig = figure(1)
    clf()
    ax = Axes3D(fig)
    for i, c, label in zip(target_ids, colors, target_names):
        ax.plot(data[target == i, 0], data[target == i, 1], data[target == i, 2],
                  'o', c=c, label=label)
    legend()
    show()

def _analyze_mos(proc_mos, **kwargs):
    '''
    Extract the desired features from a processed MOS entry
    '''
    # Extract keyword arguments
    day1 = kwargs['day1']

    # Group by date
    g = proc_mos.groupby(proc_mos.index.map(lambda x: x.day))
    # Select the Day 1 forecast
    fcst_day1 = g.get_group(day1.day)
    fcst_day1.Tmax = proc_mos.maxmins[day1]['max']
    fcst_day1.Tmin = proc_mos.maxmins[day1]['min']
    fcst_day1.qpf = proc_mos.precip[day1]['Q12']
    fcst_day1.pop = proc_mos.precip[day1]['P12']

    return fcst_day1

def aggregate_mos(station, interval):
    '''
    Aggregate the forecast data and observations.

    Will pull from two sources: 18Z GFS and 12Z NAM. Only collects
    the Day 1 forecasts.
    '''
    start_date, end_date = interval
    day_count = (end_date - start_date).days + 1

    path_to_mos_data = os.path.join(os.getcwd(), "data_arch", station)

    ## 1) Collect the entire Day 1 forecasts.
    all_gfs_fcsts = []
    all_nam_fcsts = []
    dates = [start_date + datetime.timedelta(n) for n in range(day_count)]
    for date in dates[:-1]:
        y, m, d = date.year, date.month, date.day
        ## Only do January
        #if m != 1: continue
        print date
        day1 = date + datetime.timedelta(1)

        ## GFS 18Z
        # Read raw MOS data
        fn = "%s.%02d%02d%4d.%s.%02dZ" % (station, m, d, y, mos.full_model_name["GFS"], 18)
        f = open(os.path.join(path_to_mos_data, "GFS", fn), "r")
        # Process MOS data
        proc_mos = mos.process_MOS(f.readlines())
        gfs_day1 = _analyze_mos(proc_mos, day1=day1)
        # Append
        all_gfs_fcsts.append(gfs_day1)

        ## NAM 12Z
        # Read raw MOS data
        fn = "%s.%02d%02d%4d.%s.%02dZ" % (station, m, d, y, mos.full_model_name["NAM"], 12)
        f = open(os.path.join(path_to_mos_data, "NAM", fn), "r")
        # Process MOS data
        proc_mos = mos.process_MOS(f.readlines())
        nam_day1 = _analyze_mos(proc_mos, day1=day1)
        # Append
        all_nam_fcsts.append(nam_day1)

    ## 2) Calculate the predictor variables:
    all_fcst_predictors = []
    for fcsts in [all_gfs_fcsts, all_nam_fcsts]:
        fcst_predictors = []
        for fcst in fcsts:
            predictors = []

            # 1) Tmax
            predictors.append(fcst.Tmax)

            # 2) Tmin
            predictors.append(fcst.Tmin)

            # 3) Rain
            predictors.extend(fcst.qpf)
            predictors.extend(fcst.pop)

            # 4) Snow
            # not implemented yet!

            # 5) Wind Direction
            # Will categorize:
            #   N = 1 : (315, 359] U [0, 45]
            #   E = 2 : (45, 135]
            #   S = 3 : (135, 225]
            #   W = 4 : (225, 315]
            avg_wind_dir = (fcst.WDR*10.).mean()
            def dir_flag(wdr):
                if (45 < wdr) and (135 <= wdr): return 1
                elif (135 < wdr) and (225 <= wdr): return 2
                elif (225 < wdr) and (315 <= wdr): return 3
                else: return 4
            flag = dir_flag(avg_wind_dir)
            predictors.append(flag)

            # 6) Average wind speed from average direction
            predictors.append(fcst.WSP.mean())

            fcst_predictors.append(predictors)
        all_fcst_predictors.append(fcst_predictors)

    ## 3) Align all the predictor variables
    all_predictors = np.hstack(all_fcst_predictors)

    ## 4) collect the observations
    variable_data, core_vars = obs.parse_observations(os.path.join(path_to_mos_data, "obs.csv"))
    observations = []
    for i, date in enumerate(dates[1:]):
        y, m, d = date.year, date.month, date.day
        ## Only do January
        #if m != 1: continue
        print date
        observations.append([core_vars.TMAX[date], core_vars.TMIN[date]])
    observations = np.array(observations)

    return all_predictors, observations

if __name__ == "__main__":

    from sklearn.ensemble import RandomForestClassifier
    from sklearn import cross_validation

    station_name = "KAUS"
    start_date = datetime.datetime(2007, 1, 1)
    end_date = datetime.datetime(2010, 12, 31)
    interval = [start_date, end_date]

    predictors, predictands = aggregate_mos(station_name, interval)
    n_samples, n_features = predictors.shape

    ## Only do TMAX for now
    predictands = predictands[:, 0]

    ## Generate labels for the data -
    # 1 = GFS High, NAM High
    # 2 = GFS High, NAM Low
    # 3 = GFS Low, NAM High
    # 4 = GFS Low, NAM Low
    categories_GFS, categories_NAM = np.zeros_like(predictands), np.zeros_like(predictands)
    gfs_tmax, nam_tmax = predictors[:, 0], predictors[:, 8]
    #categories[(gfs_tmax >= predictands) & (nam_tmax >= predictands)] = 1
    #categories[(gfs_tmax >= predictands) & (nam_tmax < predictands)] = 2
    #categories[(gfs_tmax < predictands) & (nam_tmax >= predictands)] = 3
    #categories[(gfs_tmax < predictands) & (nam_tmax < predictands)] = 4
    categories_GFS[(gfs_tmax > predictands)] = 0
    categories_GFS[(gfs_tmax < predictands)] = 1
    categories_GFS[(gfs_tmax == predictands)] = 2

    categories_NAM[(nam_tmax > predictands)] = 0
    categories_NAM[(nam_tmax < predictands)] = 1
    categories_NAM[(nam_tmax == predictands)] = 2

    predictands = categories_GFS

    ## Partition the predictors/predictands into a training/testing dataset
    indices = np.random.permutation(len(predictors))
    split = len(indices)*3/4
    training_idx, test_idx = indices[:split], indices[split:]

    X_train = predictors[training_idx]
    Y_train = predictands[training_idx]
    X_test = predictors[test_idx]
    Y_test = predictands[test_idx]

    ## Compute the classification
    forest = RandomForestClassifier(n_estimators=100, n_jobs=2, compute_importances=True)

    forest.fit(X_train, Y_train)
    Y_test_predict = forest.predict(X_test)
    print forest.score(X_test, Y_test)


    labels = ["GFS Tmax", "GFS Tmin", "GFS Q12 Early", "GFS Q12 Late", "GFS P12 Early", "GFS P12 Late", "GFS Wind Dir", "GFS Wind Speed",
              "NAM Tmax", "NAM Tmin", "NAM Q12 Early", "NAM Q12 Late", "NAM P12 Early", "NAM P12 Late", "NAM Wind Dir", "NAM Wind Speed"]

    from sklearn.decomposition import PCA
    pca = PCA(n_components=2, whiten=True).fit(X_train)
    Xt_pca = pca.transform(X_train)

    plot_2D(Xt_pca, Y_train, ["less", "greater", "equal"])
