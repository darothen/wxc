'''
Download and strip out MOS data from MDL archive
'''

from urllib2 import urlopen, URLError, HTTPError
from collections import OrderedDict
import itertools
import os, sys, subprocess, re, datetime

import numpy as np

import pandas as pd

full_model_name = { 'NAM': "NAM-MET", "GFS": "GFS-MAV" }
months = {"JAN":1, "FEB":2, "MAR":3, "APR":4, "MAY":5, "JUNE":6,
          "JULY":7, "AUG":8, "SEPT":9, "OCT":10, "NOV":11,
          "DEC":12}

data_path = "data_arch/"

def reporthook(a,b,c):
    # ',' at the end of the line is important!
    print "% 3.1f%% of %d bytes\r" % (min(100, float(a * b) / c * 100), c),
    #you can also use sys.stdout.write
    #sys.stdout.write("\r% 3.1f%% of %d bytes"
    #                 % (min(100, float(a * b) / c * 100), c)
    sys.stdout.flush()

def station_headers(mos_file, station_id, model="GFS"):
    lines = mos_file.readlines()
    for i, line in enumerate(lines):
        if station_id in line:
            # seek forward to find end of station data
            for i_end in xrange(i+1, i+30):
                if "%s MOS GUIDANCE" % model in lines[i_end]: break
            yield lines[i:i_end]

def download_file(url, local_filename):
    print "downloading " + url,
    f = urlopen(url)
    with open(local_filename, "wb") as local_file:
        local_file.write(f.read())

def get_NAM(station, years):
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
    ''' process a MOS block

    This function should accept a list containing each line
    in a MOS entry from GFS or NAM. It analyzes the header
    of the MOS forecast to discover the forecast station and
    forecast issuing time. Then, it strips all the Day 1 and
    Day 2 forecast information out of the MOS entry.

    In the future, capability of extracting Day 3 forecasts
    for 12Z and 18Z forecasts might be added
    '''
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

if __name__ == "__main__":
    lines = open("data_arch/KAUS/GFS/KAUS.01012009.GFS-MAV.18Z", "r").readlines()
    df = process_MOS(lines)