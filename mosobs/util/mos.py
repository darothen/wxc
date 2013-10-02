"""Collection of utilities for downloading and processing MOS output.

"""

from collections import OrderedDict
from datetime import datetime, timedelta
import itertools
import os
import re
import subprocess
import sys
from urllib2 import urlopen, HTTPError

import numpy as np
import pandas as pd

full_model_name = { 'NAM': "NAM-MET", "GFS": "GFS-MAV" }
months = {"JAN":1, "FEB":2, "MAR":3, "APR":4, "MAY":5, "JUNE":6,
          "JULY":7, "AUG":8, "SEPT":9, "OCT":10, "NOV":11,
          "DEC":12}

data_path = "data_arch/"

def reporthook(a,b,c):
    """Custom download progress bar.

    """
    # ',' at the end of the line is important!
    print "% 3.1f%% of %d bytes\r" % (min(100, float(a * b) / c * 100), c),
    #you can also use sys.stdout.write
    #sys.stdout.write("\r% 3.1f%% of %d bytes"
    #                 % (min(100, float(a * b) / c * 100), c)
    sys.stdout.flush()

def station_headers(mos_file, station_id, model="GFS"):
    """Seek forward from block to block in compilation of MOS data.

    """
    lines = mos_file.readlines()
    for i, line in enumerate(lines):
        if station_id in line:
            # seek forward to find end of station data
            for i_end in xrange(i+1, i+30):
                if "%s MOS GUIDANCE" % model in lines[i_end]: break
            yield lines[i:i_end]

def download_file(url, local_filename):
    """Convenience utility for downloading and saving a file to disk.

    """
    print "downloading " + url,
    f = urlopen(url)
    with open(local_filename, "wb") as local_file:
        local_file.write(f.read())

def get_NAM(station, years):
    """Download NAM MOS output.

    Queries the NWS MDL to download NAM MOS output (00Z and 12Z) 
    for a given station over a specified range of years, and saves 
    single-block outputs of each forecast locally. 

    For example, to download all the NAM MOS forecasts from 2009 for
    Lousville's Standiford Field forecast location, one can execute 
    from an interactive Python session, 

    >>> get_NAM("KSDF", [2009, ])
    Downloading MOS data for KSDF
    http://www.mdl.nws.noaa.gov/~mos/archives/etamet/met200901.Z 
    Creating data_arch/KSDF/NAM
    done
    http://www.mdl.nws.noaa.gov/~mos/archives/etamet/met200902.Z 
    ...

    Parameters
    ----------
    station : string
        Station identifier code
    years : iterable of int
        The calendar years of data to download

    Raises
    ------
    HTTPError
        If the requested data could not be downloaded.

    """
    print "Downloading MOS data for", station
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

def get_GFS(station, years):    
    """Download GFS MOS output.

    Queries the NWS MDL to download GFS MOS output (00Z, 06Z, 12Z, 18Z) 
    for a given station over a specified range of years, and saves 
    single-block outputs of each forecast locally. 

    For example, to download all the NAM MOS forecasts from 2009 for
    Lousville's Standiford Field forecast location, one can execute 
    from an interactive Python session, 

    >>> get_GFS("KSDF", [2009, ])
    Downloading MOS data for KSDF
    http://www.mdl.nws.noaa.gov/~mos/archives/avnmav/mav200991.t00z.Z
    Creating data_arch/KSDF/NAM
    done
    http://www.mdl.nws.noaa.gov/~mos/archives/avnmav/mav200991.t06z.Z
    ...

    Parameters
    ----------
    station : string
        Station identifier code
    years : iterable of int
        The calendar years of data to download

    Raises
    ------
    HTTPError
        If the requested data could not be downloaded.

    """
    print "Downloading MOS data for", station
    months = range(1, 13)

    for (year, month) in itertools.product(years, months):
        for fcst_time in [0, 6, 12, 18]:
            ## 18Z
            link = "http://www.mdl.nws.noaa.gov/~mos/archives/avnmav/mav%4d%02d.t%02dz.Z" % (year, month, fcst_time)

            print link
            ## Download file
            temp_file_name = link.split("/")[-1]

            ## Make the path to store the data
            full_path = os.path.join(data_path, station, "GFS")
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

            sh = station_headers(f, station, "GFS")
            for mos_lines in sh:

                station_id, _, _, _, run_date, run_time, _ = mos_lines[0].split()
                rd_month, rd_day, rd_year = map(int, run_date.split("/"))
                mos_filename = "%s.%02d%02d%4d.GFS-MAV.%02dZ" % (station_id, rd_month, rd_day, rd_year, fcst_time)

                print mos_filename
                new_f = open("data_arch/%s/GFS/%s" % (station_id, mos_filename), 'wb')
                new_f.writelines(mos_lines)
                new_f.close()

            f.close()
            os.remove(uncomp_fn)

def process_MOS(lines):
    """Process a block of MOS output.

    This method converts a block of raw MOS output into a
    timeseries format using a pandas DataFrame. It accepts
    the raw, individual lines of MOS output (such as the contents
    of a file written to disk by `get_NAM()`). The header of the block
    is analyzed to format the indices of the timeseries, and then it strips
    out Day 1 and Day 2 max/min/precip forecasts while preserving all
    3-hourly forecast data.

    Parameters
    ----------
    lines : list of strings
        Individual lines comprising a MOS block forecast

    Returns
    -------
    pandas.DataFrame
        MOS forecast

    .. note:: The output has two special attributes, `maxmin` and `precip`
        which contained processed Day 1 and Day 2 specific forecasts.

    """
    lines = map(lambda x: x.strip(), lines)

    # Line 1 - header info
    station_id, _, _, _, run_date, run_time, _ = lines[0].split()
    month, day, year = map(int, run_date.split("/"))
    hour = int(run_time)/100
    fcst_datetime = datetime.datetime(year, month, day, hour)

    meta = {'station': station_id, 'run': fcst_datetime}

    # Line 2 - forecast dates
    forecast_dates = map(lambda x: x.strip(), lines[1].split("/")[1:])[::-1]
    slash_indices = [m.start() for m in re.finditer("/", lines[1])]

    # Line 3 - forecast hours
    forecast_hours = map(int, lines[2].split()[1:])
    line_length = len(lines[2])

    # Line 4 - max/min temps
    max_min = map(int, lines[3].split()[1:])

    # Remaining lines
    other_data = {}
    minmax = []
    ## Extract field name
    for line in lines[3:]:
        ## Pad the end of the line if its too short, i.e. the strip operation nicked off
        ## the last hour of data like can happen sometimes with the 12-hr fields
        if len(line) < line_length: line += " "*(line_length - len(line))

        name = line[:3]

        ## Switch based on the name
        if name in ["TMP", "DPT", "CLD", "WDR", "WSP", "CIG", "VIS", "OBV", "POS", "POZ", "TYP"]:
            data = [line[i:i+3] for i in xrange(4, len(line), 3)]

            f = str if name in ["OBV", "CLD", "TYP"] else int
            data = map(f, data)

            other_data[name] = data
        elif name in ["X/N", "N/X"]:

            data = [line[i:i+3] for i in xrange(4, len(line), 3)]
            data = [int(d) for d in data if d.strip()]

            minmax = data
        elif name in ["P06", "Q06", "P12", "Q12"]:
            data = [line[i:i+3] for i in xrange(4, len(line), 3)]

            def f(x):
                if not x.strip():
                    return np.NaN
                else:
                    return int(x)
            data = map(f, data)

            other_data[name] = data
        else: continue

    # Map the forecast_hours entries to specific dates/times
    proc_timestamps = []
    cds = []
    for i, hour in enumerate(forecast_hours):
        # If first entry
        if i == 0:
            if (month == 12) and (day == 31) and (fcst_datetime.hour == 18):
                current_year = fcst_datetime.year + 1
            else:
                current_year = fcst_datetime.year
            popped_date = forecast_dates.pop()
            month, day = popped_date.split()
            month, day = months[month], int(day)
            cd = datetime.datetime(current_year, month, day)
            cds.append(cd)

        if (i != 0) and (hour == 0):
            cd += datetime.timedelta(days=1)
            cds.append(cd)
        timestamp = datetime.datetime(cd.year, cd.month, cd.day, hour)
        proc_timestamps.append(timestamp)

    df = pd.DataFrame(other_data, index=proc_timestamps)
    df.meta = meta

    ## Map the max/mins to their appropriate dates, as well as the
    maxmins = OrderedDict()
    if fcst_datetime.hour < 12:
        day0, day1, day2, day3 = cds
        maxmins[day0] = {'max': minmax[0]}
        maxmins[day1] = {'min': minmax[1], 'max': minmax[2]}
        maxmins[day2] = {'min': minmax[3], 'max': minmax[4]}
    elif fcst_datetime.hour >= 12:
        if fcst_datetime.hour == 12: day0, day1, day2, day3 = cds
        if fcst_datetime.hour == 18: day1, day2, day3 = cds
        maxmins[day1] = {'min': minmax[0], 'max': minmax[1]}
        maxmins[day2] = {'min': minmax[2], 'max': minmax[3]}
        maxmins[day3] = {'min': minmax[4]}
    df.maxmins = maxmins

    ## Map the 12hr POP/QPF forecasts to their appropriate dates (shift them to the *beginning*)
    ## of their forecast interval
    precip = OrderedDict()
    precip_df = df[["Q12", "P12"]].dropna()
    precip_index = precip_df.index
    precip_df = precip_df.set_index(precip_index - datetime.timedelta(hours=12))

    # Only grab the Day 1 and beyond values
    if fcst_datetime.hour < 12: precip_df = precip_df.ix[1:]
    day1, day2 = precip_df.index[:3:2]
    precip[day1] = { "Q12": [precip_df["Q12"].ix[0], precip_df["Q12"].ix[1]],
                     "P12": [precip_df["P12"].ix[0], precip_df["P12"].ix[1]]  }
    precip[day2] = { "Q12": [precip_df["Q12"].ix[2], precip_df["Q12"].ix[3]],
                     "P12": [precip_df["P12"].ix[2], precip_df["P12"].ix[3]]  }
    df.precip = precip

    return df

    full_model_name = { 'NAM': "NAM-MET", "GFS": "GFS-MAV" }
    months = {"JAN":1, "FEB":2, "MAR":3, "APR":4, "MAY":5, "JUNE":6,
              "JULY":7, "AUG":8, "SEPT":9, "OCT":10, "NOV":11,
              "DEC":12}
    data_path = "data_arch/"

    from datetime import datetime, timedelta
    import itertools

def concatenate_MOS(station, forecast_date, gfs=True, nam=True):
    """ Concatenate MOS forecasts from GFS and NAM corresponding to
    a given forecast date.
    
    Forecast period is implied to be 00Z on the forecast date to
    00Z on the following day. This method will align the 00Z-18Z 
    forecasts on the previous day to get a timeseries/snapshot of the
    forecast for the forecast date. 
    
    Example usage:
    
    >>> forecast_date = datetime.strptime("2008-09-17", "%Y-%m-%d")
    
    Parameters
    ----------
    station : string
        Station identifier code
    forecast_date : datetime.datetime
        The date corresponding to the 00z-00z forecast period
    gfs, nam : boolean, optional
        Logical switch on which model to include (both true by
        default)
    
    Returns
    -------
    pandas.DataFrame
        Aligned MOS forecasts
        
    """
    mos_pattern = "%s.%02d%02d%4d.%s.%02dZ" # station, mm, dd, yyyy, model, hh
    one_day = timedelta(days=1)
    
    ## Generate filenames of MOS data to read
    model_date = forecast_date - one_day
    md = model_date # alias
    
    hours = [0, 6, 12, 18]
    models = []
    if gfs: models.append("GFS")
    if nam: models.append("NAM")
    
    combos = itertools.product(hours, models)
    all_mos = {}
    for hour, model in combos:
        filename = mos_pattern % (station, md.month, md.day, md.year,
                                  full_model_name[model], hour)
        print "Processing", filename
        
        full_path = os.path.join(data_path, station, model, filename)
        if not os.path.exists(full_path):
            print "   could not find file; skipping"
            continue
            
        model_fcst_date = datetime(md.year, md.month, md.day, hour)
        with open(full_path, 'r') as f:
            proc_mos = process_MOS(f.readlines())
        
        fcst_name = "%s%02dZ" % (model, hour)
        all_mos[fcst_name] = proc_mos
    
    mos_panel = pd.Panel(all_mos)
    
    return mos_panel, all_mos

if __name__ == "__main__":
    lines = open("data_arch/KAUS/GFS/KAUS.01012009.GFS-MAV.18Z", "r").readlines()
    df = process_MOS(lines)