import numpy as np
import unittest

save_file = "all_penalties.npy"

class PrecipPenaltyTest(unittest.TestCase):

    def testA(self):
        fcst, obs = .17, .30
        self.assertAlmostEqual(penalty(fcst, obs), 3.4)

    def testB(self):
        fcst, obs = .17, .65
        self.assertAlmostEqual(penalty(fcst, obs), 8.9)

    def testC(self):
        fcst, obs = .17, .02
        self.assertAlmostEqual(penalty(fcst, obs), 5.3)

    def testD(self):
        fcst, obs = .90, 0.70
        self.assertAlmostEqual(penalty(fcst, obs), 2.0)

    def testE(self):
        fcst, obs = .90, .45
        self.assertAlmostEqual(penalty(fcst, obs), 5.0)

    def testF(self):
        fcst, obs = 0.9, 0.
        self.assertAlmostEqual(penalty(fcst, obs), 17.5)

    def testG(self):
        fcst, obs = 1.07, .80
        self.assertAlmostEqual(penalty(fcst, obs), 2.7)

def penalty(fcst, obs):
    """Calculate penalty given a pair of forecast and observed precipitation
    measurements.

    Taken from http://wxchallenge.com/info/rules.php. Both the forecast and
    observation should be provided in hundredths of an inch.

    Parameters
    ----------
    fcst : real
    obs : real

    Returns
    -------
    penalty : real
        Calculated penalty given the forecast and observation.

    Raises
    ------
    ValueError
        If either fcst or obs are negative.
    """

    if (fcst < 0.) or (obs < 0.):
        raise ValueError("Either fcst or obs was negative")

    ## Adjust the fcst and obs by flooring to the nearest hundredth
    fcst = np.floor(fcst*100.)
    obs = np.floor(obs*100.)

    ## Compile the accumulated penalties for each verification range
    total_penalty = 0.

    bounds = [0., 10., 25., 50., 1e6]
    bins = zip(bounds[:-1], bounds[1:])
    factors = [0.4, 0.3, 0.2, 0.1]

    fcst_bin, obs_bin = 0, 0
    for i, (a, b) in enumerate(bins):
        if a < fcst <= b: fcst_bin = i#; print "fcst ->", i
        if a < obs <= b: obs_bin = i#; print "obs ->", i

    if fcst_bin == obs_bin:
        total_penalty += factors[fcst_bin]*np.abs(fcst - obs)

    elif fcst_bin < obs_bin:
        #print "fcst_bin < obs_bin"
        for bin in range(fcst_bin, obs_bin+1):
            left, right = bins[bin]

            #print left, right, factors[bin],

            if left < obs <= right:
                total_penalty += factors[bin]*(obs - left)
                #print "(%2d - %2d)" % (obs, left)
            elif left < fcst <= right:
                total_penalty += factors[bin]*(right - fcst)
                #print "(%2d - %2d)" % (right, fcst)
            else: 
                total_penalty += factors[bin]*(right - left)
                #print "(%2d - %2d)" % (right, left)
    else:
        #print "fcst_bin > obs_bin"
        for bin in range(obs_bin, fcst_bin+1):
            left, right = bins[bin]

            #print left, right, factors[bin],

            if left < obs <= right:
                total_penalty += factors[bin]*(right - obs)
                #print "(%2d - %2d)" % (right, obs)
            elif left < fcst <= right:
                total_penalty += factors[bin]*(fcst - left)
                #print "(%2d - %2d)" % (fcst, left)
            else:
                total_penalty += factors[bin]*(right - left)
                #print "(%2d - %2d)" % (right, left)
    
    return total_penalty

if __name__ == "__main__":

    import os

    from pylab import *
    ion()

    #unittest.main()

    vec_penalty = np.vectorize(penalty)

    p_max = 1.0

    fcsts = np.arange(0, p_max+0.01, 0.01)
    obss = np.arange(0, p_max+0.01, 0.01)

    if not os.path.exists(save_file):
        print "Computing and saving penalties"
        all_fcsts, all_obs = np.meshgrid(fcsts, obss)
        all_penalties = vec_penalty(all_fcsts, all_obs)

        np.save(save_file, all_penalties)
    else:
        print "Loading saved penalties"
        all_penalties = np.load(save_file)

    fig = figure(1, figsize=(16, 12)); clf()

    left, width = 0.1, 0.75
    bottom, height = 0.1, 0.75

    rect_main = [left, bottom, width, height]
    rect_top = [left, bottom+height+0.02, width, 0.1]
    rect_right = [left+width+0.02, bottom, 0.1, height]
    rect_bot = [left, 0.02, width, 0.02]

    ax_main = axes(rect_main)
    ax_top = axes(rect_top)
    ax_right = axes(rect_right)
    ax_bot = axes(rect_bot)

    grid = ax_main.pcolormesh(fcsts, obss, all_penalties)
    cb = fig.colorbar(grid, cax=ax_bot, orientation='horizontal')

    ax_main.set_xlim(0, p_max)
    ax_main.set_xlabel("Forecast Precip (in)")
    ax_main.set_ylim(0, p_max)
    ax_main.set_ylabel("Verified Precip (in)")

    exit = False
    
    glyph,  = ax_main.plot([], [], 'xk', markersize=15, markeredgewidth=3)

    def update_plots(fcst, obs):
        stat_pen = penalty(fcst, obs)

        ## Update plot on right
        ax_right.cla()
        fcst_exp = np.ones_like(fcsts)*fcst
        fcst_pen = vec_penalty(fcst_exp, obss)
        ax_right.plot(fcst_pen, obss, 'k')
        ax_right.hlines([obs], 0, 18, linestyle='dashed')
        ax_right.set_ylim(ax_main.get_ylim())
        ax_right.set_yticks([])

        ## Update plot on top
        ax_top.cla()
        obs_exp = np.ones_like(obss)*obs
        obs_pen = vec_penalty(fcsts, obs_exp)
        ax_top.plot(fcsts, obs_pen, 'k')
        ax_top.vlines([fcst], 0, 18, linestyle='dashed')
        ax_top.set_xlim(ax_main.get_xlim())
        ax_top.set_xticks([])

        ## Update main plot
        glyph.set_xdata(fcst+0.005)
        glyph.set_ydata(obs+0.005)

        ## Other settings
        ax_top.set_ylim(0, 18)
        ax_right.set_xlim(0, 18)

        draw()  

    update_plots(0.5, 0.5)

    while not exit:
        inp = raw_input("fcst, obs | (leave blank to exit) ")
        if not inp:
            exit = True; break

        bits = inp.split(",")
        fcst, obs = map(lambda s: float(s.strip()), bits)

        exp_penalty = penalty(fcst, obs)
        print "   Expected penalty = %1.1f" % exp_penalty

        update_plots(fcst, obs)
