import sys
import numpy as np
import datetime
from pandas import *
from pylab import *
from mos import download_file, data_path
ion()

## Pre-defined flags
MISSING = 9999

STATIONS = { 'ksdf': 'LOUISVILLE INTERNATIONAL AIRPORT KY US',
             'ksyr': 'SYRACUSE HANCOCK INTERNATIONAL AIRPORT NY US', }

# 1 day timedelta
ONE_DAY = datetime.timedelta(days=1)

def get_OBS(station, years):
    print "Downloading OBS data for", station
    #years = range(2009, 2012)
    months = range(1, 13)

    for (year, month) in itertools.product(years, months):
        link = "http://www.mdl.nws.noaa.gov/~mos/archives/etamet/met%4d%02d.Z" % (year, month)
        print link
        ## Download file
        temp_file_name = link.split("/")[-1]

        full_path = os.path.join(data_path, station, "NAM")
        ## Make the path to store the data
        if not os.path.exists(full_path):
            print "Creating", full_path
            os.makedirs(full_path)
        full_fn = os.path.join(data_path, temp_file_name)

        if not os.path.exists(full_fn):
            try:
                download_file(link, full_fn)
                print "done"
            except HTTPError, e:
                print "...resource not found. Skipping."
                continue

            ## Uncompress file
            subprocess.call(["uncompress", full_fn])
        uncomp_fn = full_fn[:-2] # trim the ".Z"

        f = open(uncomp_fn)

        sh = station_headers(f, station, "NAM")
        for mos_lines in sh:

            station_id, _, _, _, run_date, run_time, _ = mos_lines[0].split()
            fcst_time = int(run_time)/100
            rd_month, rd_day, rd_year = map(int, run_date.split("/"))
            mos_filename = "%s.%02d%02d%4d.NAM-MET.%02dZ" % (station_id, rd_month, rd_day, rd_year, fcst_time)

            print mos_filename
            new_f = open("data_arch/%s/NAM/%s" % (station_id, mos_filename), 'wb')
            new_f.writelines(mos_lines)
            new_f.close()

        f.close()
        os.remove(uncomp_fn)

def parse_mos(filename):
    """Right now, will only strip out min/max temps for 0-day forecast

    assume 18Z data is being supplied
    """
    mos_file = open(filename, 'r')
    lines = mos_file.readlines()

    forecast_data = {}

    ## Split the lines for further processing
    header = lines[0]
    fcst_dates = lines[1]
    fcst_hrs = lines[2]
    data = lines[3:]

    ## 1) Parse the station header and model init time
    header_bits = header.split()
    station_id = header_bits[0]
    init_date = header_bits[4]
    init_dd, init_mm, init_yy = map(int, init_date.split("/"))
    init_time = int(header_bits[5])

    ## 2) Grab 0-day min/max
    minmaxes = data[0].split()
    tmax, tmin = map(int, minmaxes[1:3])

    return tmax, tmin


def parse_observations(filename, station_name=None):
    ## Use pandas' csv reader to quickly read the observational dataset into a
    ## managable format
    data = read_csv(filename)
    if station_name:
        data = data[data.STATION_NAME == station_name]

    ## Grab station metadata
    station_id = data.STATION.values[0]
    elevation = data.ELEVATION.values[0]
    latitude = data.LATITUDE.values[0]
    longitude = data.LONGITUDE.values[0]

    ## Grab station dates and format them as indices for the new, parsed data
    raw_dates = data.DATE
    dates = []
    for date in raw_dates:
        date = str(date)
        yy, mm, dd = map(int, [date[:4], date[4:6], date[6:8]])
        dates.append(datetime.datetime(yy, mm, dd))

    ## Iterate over the columns and generate a DataFrame for each type of data
    columns = data.columns[6:]
    num_vars = len(columns)/5
    variable_data = {}
    for i in xrange(num_vars):
        var_index = i*5
        var_name = columns[var_index]
        print var_name
        #if var_name != 'TMAX': continue
        var_data = data[columns[var_index]].values

        measurement_flag = data[columns[var_index+1]].values
        quality_flag = data[columns[var_index+2]].values
        source_flag = data[columns[var_index+3]].values
        time_of_obs = data[columns[var_index+4]].values

        ## Deal with missing and flagged data
        # 1) Missing - replace all missing values with nulls
        #var_data = np.ma.masked_equal(var_data, MISSING)
        # 2) Replace any quality-flagged values with nulls
        #var_data[quality_flag != ' '] = np.nan

        ## Construct the DataFrame
        data_df = DataFrame({'data': var_data, 'meas_flag': measurement_flag,
            'qual_flag': quality_flag, 'src_flag': source_flag}, index=dates)

        variable_data[var_name] = data_df

    ## Take the core fields and convert them to the appropriate units
    # SNOW: mm, N/A
    # SNWD: mm, N/A
    # PRCP: tenths of mm -> mm (PRCP/10.)
    # TMAX/TMIN: tenths of deg C -> deg F
    #(9/5)*Tc+32
    core_df_fields = {'SNOW': variable_data['SNOW'].data,
                      'SNWD': variable_data['SNWD'].data,
                      'TMAX': (9./5.)*(variable_data['TMAX'].data/10.) + 32.,
                      'TMIN': (9./5.)*(variable_data['TMIN'].data/10.) + 32.,
                      'PRCP': variable_data['PRCP'].data/10.}
    core = DataFrame(core_df_fields)
    return variable_data, core

if __name__ == "__main__":

    id = sys.argv[1]

    station_name = STATIONS[id]
    print station_name
    variable_data, core = parse_observations("%s_daily_obs.csv" % id, station_name)

    tmaxes, tmins = [], []
    for date in core.index:
        # Decrement the day by "1" - we want to grab the MOS forecast from the *previous day*
        # since this is the observation to validate it.
        date = date-ONE_DAY
        filename = "data_arch/%s/%s.%02d%02d%04d.GFS-MAV.18Z" % (id, id, date.month, date.day, date.year)
        try:
            tmin, tmax = parse_mos(filename)
        except IOError, e:
            print "Could not find MOS file:", filename
            tmin, tmax = None, None
        tmaxes.append(tmax)
        tmins.append(tmin)
    mos = DataFrame({'TMAX': tmaxes, 'TMIN': tmins}, index=core.index)

    ## group by month
    mos_grouped = mos.groupby([lambda x: x.year, lambda x: x.month])
    core_grouped = core.groupby([lambda x: x.year, lambda x: x.month])

    def rmse(fcst, obs, varname):
        sqe = np.power(fcst[varname] - np.floor(obs[varname]), 2)
        msqe = np.mean(sqe)
        rmsqe = np.sqrt(msqe)
        return rmsqe

    for mg, cg in zip(mos_grouped, core_grouped):
        (year, month), fcst = mg
        (_, _), obs = cg

        rms = rmse(fcst, obs, 'TMAX')
        print year, month, rms
