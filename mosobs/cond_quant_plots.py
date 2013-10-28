"""
Constructs conditional-quantile plots for evaluating MOS forecast accuracy and calibration
"""
import numpy as np
import pandas as pd
from util.obs import parse_observations
from util.mos import concatenate_MOS
from pylab import *
ion()

import sys, time

#model_keys = ["GFS00Z", "GFS06Z", "GFS12Z", "GFS18Z", "NAM00Z", "NAM12Z"]
model_keys = ["GFS12Z", "GFS18Z", "NAM12Z"]

station = "KOKC"
vd, core = parse_observations(station)

## Subset the observational data. As an example, choose all
## October data
core_ss = core[(core.index.month == 10) | (core.index.month == 11)]


dates = core_ss.index
valid_dates = []
fcst_tmaxes = dict()
for key in model_keys:
    fcst_tmaxes[key] = []

print fcst_tmaxes

for i, d in enumerate(dates):
   sys.stdout.write("\r%2.1f%%" % (100.*float(i)/len(dates)))
   sys.stdout.flush() 

   try:
        mos_panel, mos_fcst = concatenate_MOS(station, d)
   except ValueError:
        continue

   valid_dates.append(d)
   for key in model_keys:
        try:
            mos_maxmin = mos_fcst[key].maxmins[d]['max']
            fcst_tmaxes[key].append(mos_maxmin)
        except KeyError:
            fcst_tmaxes[key].append(np.nan)
       
obs_tmaxes = np.floor(core_ss['TMAX'].ix[valid_dates])
fcst_tmaxes.update({'obs': obs_tmaxes})

data = pd.DataFrame(fcst_tmaxes)      

################################
for model in model_keys:
    data_slice = data[["obs", model]]
    data_slice = data_slice.dropna()

    ## Calculate quantiles
    # Gather the data for each forecast value:
    fcst_groups = data_slice.groupby(model)
    fcsts, obs = [], []
    for fcst_tmax, group in fcst_groups:
        #print fcst_tmax, "\n", group
        fcsts.append(fcst_tmax)
        obs.append(group['obs'].values.tolist())
        
    # Use percentile function in numpy
    from scipy.stats.mstats import mquantiles
    quants = []
    cor_fcsts = []
    for fcst, obs_set in zip(fcsts, obs): 
        #if len(obs_set) <= 5: continue
        q = mquantiles(obs_set, [0.1, 0.25, .5, 0.75, .9])
        quants.append(q)
        cor_fcsts.append(fcst)
        print fcst, q
    quants = np.array(quants)
    print quants.shape
    print len(cor_fcsts)

    # Smooth using LOWESS
    import statsmodels.api as sm
    lowess = sm.nonparametric.lowess
    num_valid, num_quantiles = quants.shape
    all_quants = np.zeros([len(fcsts), num_quantiles])
    for i in xrange(num_quantiles):
        fi = lowess(quants[:, i], cor_fcsts, return_sorted=False,
                    frac=1./12., it=0)
        all_quants[:, i] = fi[:]

    print all_quants.shape
    quants = all_quants[:]
    for fcst, q in zip(fcsts, quants):
        print fcst, q

    tmaxes_unique = data[model].value_counts()

    ## Make frequency plot
    fig = figure()
    ax = fig.add_subplot(111)
    ax_freq = ax.twinx()

    lefts = np.array(tmaxes_unique.index) - 0.5
    heights = tmaxes_unique.values
    ax_freq.bar(lefts, heights, width=1.0)
    ax_freq.set_xlim(np.min(lefts)-4.5, np.max(lefts)+4.5)
    ax_freq.set_ylim(0, np.max(heights)*3)
    ax_freq.grid(False)
    ax_freq.set_ylabel("Sample Size")

    ## Make quantile curves
    xs = np.linspace(*ax_freq.get_xlim(), num=100)
    ax.plot(xs, xs, color='gray')
    ## 0.1, 0.9
    p1, _ = ax.plot(fcsts, quants[:, [0, -1]], ':k')
    p25,_ = ax.plot(fcsts, quants[:, [1, -2]], '--k')
    p5  = ax.plot(fcsts, quants[:, 2], '-k')
    p5 = p5[0] # Choose from list
    ax.legend([p1, p25, p5], 
              ["0.1/0.9 quantile", "0.25/0.75 quantile", "0.5 quantile"],
              loc='upper left', fontsize=10)
    ax.set_xlabel("Forecast Temperature ($^o$F)")
    ax.set_ylabel("Observed Temperature ($^o$F)")

    ax.set_title("%s - %s" % (model, station), loc="left")
    savefig("%s.%s.pdf" % (station, model), transparent=True, bbox_inches="tight")
