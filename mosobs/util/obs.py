import sys
import numpy as np
import datetime
import os
from pandas import *
from pylab import *
from urllib2 import HTTPError
from mos import download_file, data_path
ion()

## Pre-defined flags
MISSING = 9999

STATIONS = { 'ksdf': 'LOUISVILLE INTERNATIONAL AIRPORT KY US',
             'ksyr': 'SYRACUSE HANCOCK INTERNATIONAL AIRPORT NY US', }

# 1 day timedelta
ONE_DAY = datetime.timedelta(days=1)

# NWSO location closest to each station?
STATION_CODES = { 'khou': 'USC00414333',
                  'kcys': 'USC00481676',
                  'nrmn': 'USC00346382', 
                  'kgrr': 'USC00203337',
                  }

def code_from_station(station):
    # translate 4 letter station id to hcn code
    return STATION_CODES[station.lower()]

def fetch_OBS(station, update='True'):
    '''Script for downloading the observations for the station and 
    archiving them in a subdirctory of the data_path defined by the 
    other utils.
    
    ATMo only works for stations who have an ID translation in the 
    STATION_CODES dictionary'''
    
    print "Downloading OBS data for", station
    station_code = code_from_station(station) # switching ID systems
    link = "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/all/%s.dly" % (station_code)

    ## Download file
    temp_file_name = link.split("/")[-1]
    full_path = os.path.join(data_path, station, "OBS")
    ## Make the path to store the data
    if not os.path.exists(full_path):
        print "Creating", full_path
        os.makedirs(full_path)
    full_fn = os.path.join(full_path, temp_file_name)

    if (not os.path.exists(full_fn)) or (update==True):
        try:
            download_file(link, full_fn)
            print "done"
        except HTTPError, e:
            print "...resource not found. Skipping."

    f = open(full_fn)
    
    ## Put data in archivable format
    obs_data = {'TMAX':{'t':[],'data':[], 'flags':[]},
                'TMIN':{'t':[],'data':[], 'flags':[]},
                'PRCP':{'t':[],'data':[], 'flags':[]}
                }
    for line in f.readlines():
        yyyy = line[11:15] #file formatted particularly by text line, column
        mm = line[15:17]
        var_id = line[17:21]
        if var_id in obs_data.keys():
            rest = line[21:]
            for d in range(31):
                try: 
                    dt = datetime.datetime(int(yyyy),int(mm),int(d+1))
                except ValueError:
                    break
                obs_data[var_id]['t'].append(dt)   
                
                if var_id in ['TMAX', 'TMIN']:
                    datum = float(rest[d*8:d*8+5])
                    if datum == -9999.:
                        obs_data[var_id]['data'].append(np.nan)
                    else:
                        # to deg C
                        obs_data[var_id]['data'].append(datum*0.1*(9./5.)+32.)
                elif var_id == 'PRCP':
                    datum = float(rest[d*8:d*8+5])
                    if datum == -9999.:
                        obs_data[var_id]['data'].append(np.nan)
                    else:
                        # to mm
                        obs_data[var_id]['data'].append(datum*0.1)
                obs_data[var_id]['flags'].append(rest[d*8+5:d*8+8])
    f.close()
    
    ## archive data by variable ID
    for var_id in obs_data:
        df = DataFrame(obs_data[var_id])
        df.to_csv(os.path.join(full_path,'%s.csv') %(var_id) )


def get_OBS(station, variable_id):
    '''Get the archived obs of variable_id type and return them as a DataFrame
    for easy use.
    e.g.  df = get_OBS('KHOU', 'TMAX')
    '''

    full_path = os.path.join(data_path, station, "OBS")
    if not os.path.exists(full_path):
        print 'No archived OBS data for this station'
        return None
    
    df = read_csv(os.path.join(full_path,'%s.csv') %(variable_id) , index_col='t')
    # format: index, variable, flag, datetime
        
    return df
    

def choose_OBS(obsdf, dtime_start, dtime_end=None, flag=False):
    '''Choose subset of total obs record according to dtime_start and dtime_end
    dtime are datetime.datetime objects
    If no dtime_end, returns single element array
    If flag set to True, return a 2d array with [data, flag] sets
    '''

    str_start = dtime_start.isoformat().replace('T', ' ')
    if dtime_end:
        str_end = dtime_end.isoformat().replace('T', ' ')
    else:
        str_end = str_start
    i0 = obsdf['Unnamed: 0'][str_start]

    if dtime_end:
        i1 = obsdf['Unnamed: 0'][str_end]
    else:
        i1 = i0

    if flag:
        return obsdf.values[i0:i1+1, 1:3]
    else:
        return obsdf.values[i0:i1+1, 1]
    
    
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
