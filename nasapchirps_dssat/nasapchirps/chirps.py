#!/usr/bin/env python
#######################################
#Script developed by Oscar Castillo
#University of Florida
#2022
#Contact email: ocastilloromero@ufl.edu
#######################################

import os, sys
from osgeo import ogr, gdal
from osgeo.gdalconst import *
import numpy
import pandas as pd
from datetime import datetime, timedelta
import joblib
import requests
from dateutil.relativedelta import relativedelta

# register all of the GDAL drivers
gdal.AllRegister()

#####Download CHIRPS data
#Corrected data
def get_correc_nc(dt_s, dt_e, out_cor_nc):
    s = requests.Session()
    s.mount("https://data.chc.ucsb.edu", requests.adapters.HTTPAdapter(max_retries=10))

    diff_month = (dt_e.year - dt_s.year) * 12 + (dt_e.month - dt_s.month)
    for n in range(diff_month+1):
        yymm = dt_s + relativedelta(months=+n)
        yy = yymm.strftime("%Y")
        mm = yymm.strftime("%m")

        try:
            #Monthly basis
            response = s.get('https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/by_month/chirps-v2.0.'
                + yy + '.' + mm + '.days_p05.nc', timeout=80)
            #Yearly basis
    #        response = s.get('https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/chirps-v2.0.'
    #                         + dt_st_y + '.days_p05.nc', timeout=80)
            response.raise_for_status()

            if response.ok:
                # Create target Directory if it doesn't exist.
                if not os.path.exists(out_cor_nc):
                    os.mkdir(out_cor_nc)
                open(out_cor_nc + '/corr_chirps_' + yy + mm + '.nc', 'wb').write(response.content)
                print('corr_chirps_' + yy + mm + ".nc file downloaded.")
    #            open(out_cor_nc + '/corr_chirps_' + dt_st_y + '.nc', 'wb').write(response.content)
    #            print('corr_chirps_' + dt_st_y + ".nc file downloaded.")

        except requests.exceptions.HTTPError as err:
            pass
#            raise SystemExit(err)

        response.close() #Close the connection with the server.

#Preliminary data
def get_prelim_nc(dt_s, dt_e, out_pre_nc):
    s = requests.Session()
    s.mount("https://data.chc.ucsb.edu", requests.adapters.HTTPAdapter(max_retries=10))

    for y in range(dt_e.year - dt_s.year + 1):
        single_y = str(dt_s.year + y)

        try:
            response = s.get('https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/fixed/netcdf/chirps-v2.0.'
                             + single_y + '.days_p05.nc', timeout=80)
            response.raise_for_status()

            if response.ok:
                # Create target Directory if it doesn't exist.
                if not os.path.exists(out_pre_nc):
                    os.mkdir(out_pre_nc)
                open(out_pre_nc  + '/prelim_nc_' + single_y + '.nc', 'wb').write(response.content)
                print('prelim_nc_' + single_y + ".nc file downloaded.")
        except requests.exceptions.HTTPError as err:
            pass
    #        raise SystemExit(err)
        response.close()  # Close the connection with the server.

#To read corrected and preliminary data and then merge them properly.
def precpkl(outdir_prec):
    df1 = joblib.load(outdir_prec + '/prec_corr.pkl')
    df2 = joblib.load(outdir_prec + '/prec_prelim.pkl')
    last_dt_corr = df1.columns[-1]
    dt_s_pre = datetime.strptime(last_dt_corr, '%Y%j') + timedelta(days=1)
    dt_st_p = dt_s_pre.strftime('%Y%j')
    df3 = df2.loc[:, dt_st_p:]
    result = pd.concat([df1, df3], axis=1)
    joblib.dump(result, outdir_prec + '/prec.pkl')

#Intended for long time series but few points (<10000)
def chirps1(in_file, in_nc_dir, outprec_file):
    nc_lst = os.listdir(in_nc_dir)  # To list all .sol files in the input folder.
    nc_lst.sort()  # Sort the files in a sequential date
    df_chirps = pd.DataFrame()

    # To read the input CSV file
    with open(in_file, "r") as f1:
        in_pt = [line for line in f1.readlines() if line.strip()]

        for row in in_pt[1:]:
            id = int(row.split(',')[0])
            lat = float(row.split(',')[1])
            lon = float(row.split(',')[2])
            time_lst = []
            precval = []

            for nc_file in nc_lst:
                if nc_file.endswith(".nc"):
                    # open the image file
                    dsi = gdal.Open(in_nc_dir + "/" + nc_file, GA_ReadOnly)

                    if dsi is None:
                        print('Could not open NetCDF file')
                        sys.exit(1)

                    meta_nc = dsi.GetMetadata()  # To get metadata of the file
                    date_start = meta_nc['time#units'][
                                 -14:]  # The origin date of the file (For CHIRPS '1980-1-1 0:0:0')
                    datetime_st = datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S')
                    bands_time = meta_nc['NETCDF_DIM_time_VALUES'][1:-1].split(
                        ',')  # Get the band timestamp point of the file. "[1:-1]" removes the first and last character
                    bands_time = list(map(int, bands_time))  # Convert all strings in a list of integers.

                    # Geotransformation
                    gt = dsi.GetGeoTransform()
                    px = int((lon - gt[0]) / gt[1])
                    py = int((lat - gt[3]) / gt[5])

                    # get image size
                    #rowsY = dsi.RasterYSize
                    #colsX = dsi.RasterXSize
                    bands = dsi.RasterCount

                    for i in range(1, bands + 1):
                        d = dsi.GetRasterBand(i).ReadAsArray(px, py, 1, 1)
                        dt_st = datetime_st + timedelta(days=bands_time[i - 1])  # Get the band timestamp in date format
                        band_t = dt_st.strftime('%Y%j')  #strftime() to reformat the date to DSSAT format.
                        time_lst.append(band_t)

                        if d is None:
                            d = numpy.float32([[-9999.0]])

                        precval.append(d[0][0])

            df_chirps[id] = precval

    df_chirps = df_chirps.T  # Array transpose
    df_chirps.index.name = 'ID'  # Set an index name
    df_chirps.columns = time_lst  # Set column names as actual dates.

    if not os.path.exists(os.path.dirname(outprec_file)):
        os.mkdir(os.path.dirname(outprec_file))
    joblib.dump(df_chirps, outprec_file)

#Intended for short time series (< 2 years) but many points. It requires more RAM memory available.
def chirps2(in_file, in_nc_dir, outprec_file):

    nc_lst = os.listdir(in_nc_dir) #To list all .sol files in the input folder.
    nc_lst.sort() #Sort the files in a sequential date
    data = {}
    time_lst = []
    start2 = datetime.now()

    #Loop through dates
    for nc_file in nc_lst:
        if nc_file.endswith(".nc"):
            start3 = datetime.now()
            # open the image file
            dsi = gdal.Open(in_nc_dir + "/" + nc_file, GA_ReadOnly)
            print(nc_file)
            if dsi is None:
                print('Could not open NetCDF file')
                sys.exit(1)

            meta_nc = dsi.GetMetadata() #To get metadata of the file
            date_start = meta_nc['time#units'][-14:] #The origin date of the file (For CHIRPS '1980-1-1 0:0:0')
            datetime_st = datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S')
            bands_time = meta_nc['NETCDF_DIM_time_VALUES'][1:-1].split(',') #Get the band timestamp point of the file. "[1:-1]" removes the first and last character
            bands_time = list(map(int, bands_time)) #Convert all strings in a list of integers.

            #Geotransformation data
            gt = dsi.GetGeoTransform()

            # get image size
            rowsY = dsi.RasterYSize
            colsX = dsi.RasterXSize
            bands = dsi.RasterCount

            for i in range(1, bands+1):
                dt_st = datetime_st + timedelta(days=bands_time[i-1]) #Get the band timestamp in date format
                band_t = dt_st.strftime('%Y%j')  # we use strftime() to reformat the date to DSSAT format.
                data[band_t] = dsi.GetRasterBand(i).ReadAsArray(0, 0, colsX, rowsY)
                time_lst.append(band_t)

            end3 = datetime.now()
            print("Time of execution for", nc_file, "is:", str(end3-start3))

    #Reads an input CSV file as an array.
    pt = pd.read_csv(in_file)
    id = pt['ID'].to_numpy()
    lon = pt['Longitude'].to_numpy()
    lat = pt['Latitude'].to_numpy()

    px = ((lon - gt[0]) / gt[1]).astype(int)
    py = ((lat - gt[3]) / gt[5]).astype(int)

    df_chirps = pd.DataFrame()
    df_chirps['ID'] = id
    df_chirps = df_chirps.set_index('ID')

    for t in time_lst:
        prec_val = data[t][py, px]
        df_chirps[t] = prec_val

    if not os.path.exists(os.path.dirname(outprec_file)):
        os.mkdir(os.path.dirname(outprec_file))

    joblib.dump(df_chirps, outprec_file)

    end2 = datetime.now()
    print("Time of execution for reading the netCDF file: ", str(end2-start2))

