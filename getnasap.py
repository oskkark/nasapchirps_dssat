#!/usr/bin/env python
#######################################
#Script developed by Oscar Castillo
#University of Florida
#2022
#Contact email: ocastilloromero@ufl.edu
#######################################

import os
import sys
import queue
import threading
import requests
import time
import logging
import pandas as pd
import joblib
from datetime import datetime

#Function to get the data from the NASAPOWER API v2
def get_data(user_input, startDate, endDate, nasa_outdir):

    # Create target Directory if it doesn't exist
    if not os.path.exists(nasa_outdir):
        os.mkdir(nasa_outdir)
    else:
        print("Directory ", nasa_outdir, " already exists. Data will be added/overwritten")

    def download(q):
        s = requests.Session()
        s.mount("https://power.larc.nasa.gov", requests.adapters.HTTPAdapter(max_retries=30))
        while True:
            pt = q.get()
            id = pt[0]
            logging.info("Requesting data for: %s", id)

            try:
                response = s.get('https://power.larc.nasa.gov/api/temporal/daily/point', params=pt[1], timeout=80)
                response.raise_for_status()
            except:
                logging.info("Error in point %s with code %s", id, response.status_code)
            else:
                write_nasawth(response.text, nasa_outdir, id)
                logging.info("Data obtained for: %s", id)

            q.task_done()
            time.sleep(delay)

    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    NUM_THREADS = 5
    delay = 0  # seconds
    q = queue.Queue()

    for i in range(NUM_THREADS):
        t = threading.Thread(target=download, args=(q,))
        t.daemon = True
        t.start()

    pt = pd.read_csv(user_input)
    pt_nasa = pt.drop_duplicates(subset=['nasapid'])

    try:
        for index, row in pt_nasa.iterrows():
            nasa_id = str(int(row['nasapid']))
            lat_np = round(row['LatNP'], 4)
            lon_np = round(row['LonNP'], 4)
            loc_param = {'parameters': 'T2M', 'community': 'AG', 'longitude': lon_np, 'latitude': lat_np,
                         'start': startDate, 'end': endDate, 'format': 'ICASA'}
            url = [nasa_id, loc_param]
            q.put(url)
        q.join()

    except KeyboardInterrupt:
        sys.exit(1)

#Function to check the NASAP files requested are downloaded in disk.
def check_files(user_input, nasa_outdir):
    # Getting the number of NASAP files requested.
    pt = pd.read_csv(user_input).drop_duplicates(subset=["nasapid"])
    pts_req = pt['nasapid'].to_list()
    n_pt = len(pts_req)
    print("Number of points requested:", n_pt)

    # List of files downloaded.
    wth_files = [int(x[:-4]) for x in os.listdir(nasa_outdir) if x.endswith(".WTH")]
    n_wth = len(wth_files)
    print("Number of points downloaded:", n_wth)

    m_pts = list(set(pts_req) - set(wth_files))
    if m_pts:
        return [m_pts, n_pt, n_wth, pt]
    else:
        print("All requested data were downloaded successfully.")
        return False

#Function to check that all NASAPOWER files requested are downloaded.
def get_data2(user_input, cf):

    if cf[2] < cf[1]:
        print(cf[1] - cf[2], "missing file(s):", cf[0])
        pt_m = cf[3].loc[cf[3]['nasapid'].isin(cf[0])]
        missing_pt = os.path.dirname(user_input) + "/missing_pt.csv"
        pt_m.to_csv(missing_pt, index=False)
        get_data(missing_pt, startDate, endDate, nasa_outdir)

    elif cf[2] > cf[1]:
        print("Something is wrong with the input file index.")
        sys.exit(1)
    else:
        print("All requested files were downloaded successfully.")

#Function to write all NASAPOWER data to WTH files
def write_nasawth(dat, out_dir, id):
    with open(out_dir + "/" + id + ".WTH", "w", newline='') as f:
        f.write(dat)

#Function to download the NASA POWER data.
def nasa(user_input, startDate, endDate, nasa_outdir):
    s1 = datetime.now()
    get_data(user_input, startDate, endDate, nasa_outdir)
    cf = check_files(user_input, nasa_outdir) #To check that all files requested were downloaded.
    if cf:
        get_data2(user_input, cf)
        if check_files(user_input, nasa_outdir):
            print('Program terminated. Please check manually NASAPOWER server response.')
            sys.exit(1)

    e1 = datetime.now()
    print("Execution time getting NASAPOWER data: ", str(e1 - s1))

#Function to merge NASAPOWER and CHIRPS data, including the quality control for SRAD.
def nasachirps(user_input, nasa_outdir, chirps_input, out_dir):

    df_prec = joblib.load(chirps_input)
    df_prec = df_prec.sort_index(axis=1)
    d = df_prec.columns.values.tolist()  # All dates available in CHIRPS
    ids_ch = df_prec.index.values.tolist()  # All IDs available in CHIRPS

    s1 = '{:>6} {:>9} {:>9} {:>7} {:>5} {:>5} {:>5} {:>5}'
    hdr1 = s1.format("@ INSI", "LAT", "LONG", "ELEV", "TAV", "AMP", "REFHT", "WNDHT" + "\n")
    s2 = '{:>7} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} {:>6} {:>6} {:>6}'
    hdr3 = s2.format("@  DATE", "T2M", "TMIN", "TMAX", "TDEW", "RHUM", "RAIN2", "WIND", "SRAD", "RAIN")

    try:
        pt = pd.read_csv(user_input)
        if out_dir is None:
            out_dir = os.path.dirname(user_input) + "/DSSAT"

        # Create target Directory if doesn't exist
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
        else:
            print("Directory ", out_dir, " already exists. Data will be overwritten")

        if nasa_outdir is None:
            nasa_outdir = os.path.dirname(user_input) + "/NASAP"

        for index, row in pt.iterrows():

            id = int(row['ID'])
            lat = round(row['Latitude'], 5)
            lon = round(row['Longitude'], 5)
            nasa_id = str(int(row['nasapid']))

            with open(nasa_outdir + "/" + nasa_id + ".WTH", "r") as f1:  # Reading nasap files
                data = [line for line in f1.readlines() if line.strip()]
                hdr2 = data[11].split()
                solar = [sr.split()[8] for sr in data[13:]]  # To get solar radiation values
                i_srad = [i for i, e in enumerate(solar) if
                          e == 'nan' or e == '-99' or e == '-99.0' or e == '-3596.4']  # To find missing srad values

                with open(out_dir + "/" + str(id) + ".WTH", "w") as f2:  # Writing requested files
                    f2.write(data[0] + '\n\n' + hdr1 + s1.format(hdr2[0], lat, lon, hdr2[3], hdr2[4], hdr2[5], hdr2[6],
                                                                 hdr2[7]) +
                             '\n\n' + hdr3 + '\n')  # Writing the header

                    for index2, row in enumerate(data[13:]):
                        c = 0  # Controls when to cutoff the end of weather file. c=1 means, the script stops writing rows.
                        r = row.split()  # row of values one line at a time

                        # SRAD quality control
                        if index2 in i_srad:
                            if ((index2 + 1 in i_srad) and (
                                    index2 + 2 in i_srad) and index2 == 0):  # Three first consecutive records have srad missing values
                                break
                            if ((
                                    index2 + 1 in i_srad) and index2 == 0):  # Two first consecutive records have srad missing values
                                break
                            if (len(data[
                                    13:]) == 1):  # The first record has srad missing value and there is no more data
                                break
                            if ((index2 + 1 in i_srad) and (index2 + 2 in i_srad)):
                                SRAD2 = solar[index2 - 1]  # Three consecutive records have srad missing values
                                c = 1
                            elif ((index2 + 1) in i_srad and (index2 + 2) == len(data[13:])):
                                SRAD2 = solar[index2 - 1]  # Two last records have srad missing values
                                c = 1
                            elif (index2 + 1) in i_srad:  # Two consecutive missing values. First value
                                SRAD2 = round(float(solar[index2 - 1]) + (
                                        float(solar[index2 + 2]) - float(solar[index2 - 1])) / 3, 1)
                            elif (index2 - 1) in i_srad:  # Two consecutive missing values. Second value
                                SRAD2 = round(float(solar[index2 - 2]) + 2 * (
                                        float(solar[index2 + 1]) - float(solar[index2 - 2])) / 3, 1)
                            elif (index2 + 1) == len(data[13:]):  # The last record has a srad missing value
                                SRAD2 = solar[index2 - 1]
                            elif (index2 == 0):  # The first record has a srad missing value but the next has valid data
                                SRAD2 = solar[index2 + 1]
                            else:
                                SRAD2 = round((float(solar[index2 - 1]) + float(solar[index2 + 1])) / 2,
                                              1)  # One consecutive missing value
                        else:
                            SRAD2 = r[8]  # To keep the original (no missing) value

                        if (r[0] in d) and (
                                id in ids_ch):  # Evaluates if date from NASA file (r[0]) is in list of dates available in dates of chirps (d) and if the ID is in the CHIRPS
                            #                            start4 = datetime.now()

                            RAIN_CHIRPS = df_prec.loc[id, r[0]]

                            if RAIN_CHIRPS == -9999.0:
                                RAIN = r[6]
                            else:
                                RAIN = round(float(RAIN_CHIRPS), 1)
                        else:
                            RAIN = r[6]

                        f2.write(s2.format(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], SRAD2, RAIN) + '\n')
                        if c == 1:
                            break

    except KeyboardInterrupt:
        sys.exit(1)
