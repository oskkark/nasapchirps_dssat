#!/usr/bin/env python
#######################################
#Script developed by Oscar Castillo
#University of Florida
#2022
#Contact email: ocastilloromero@ufl.edu
#######################################

import os, sys
import argparse
import shutil
import pandas as pd
from datetime import datetime, date, timedelta
from chirps import *
import joblib
from getnasap import nasa, nasachirps

#Select requested (.WTH) files from historical repository and copy them to a new folder.
def sel_wthfiles(in_file, in_dir, outdir_hist):
    pt = pd.read_csv(in_file)
    Id = pt.loc[:, "ID"]
    sel_files = [str(x) + ".WTH" for x in Id.to_list()] #Convert the array into a list of string elements.
    all_files = [file for file in os.listdir(in_dir)] #To get the filenames (without extension) from the "in_dir" folder

    # Create target Directory if it doesn't exist
    if not os.path.exists(outdir_hist):
        os.mkdir(outdir_hist)

    ##Look for filenames in a list and then copy them to a different folder.
    for index, wth_file in enumerate(sel_files):
        if wth_file in all_files:
            shutil.copy2(in_dir + "/" + wth_file, outdir_hist + "/" + wth_file) #To copy the corresponding file into the local directory.
        else:
            print(wth_file, " NO FOUND")

#Merge data
def mergeWTH(in_dir1, in_dir2, out_dir):
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    wth_dir2 = os.listdir(in_dir2)

    for wth_file1 in os.listdir(in_dir1):
        if wth_file1.endswith(".WTH"):
            if wth_file1 in wth_dir2:
                #To make a copy of the first file
                shutil.copy(in_dir1 + "/" + wth_file1, out_dir + "/" + wth_file1)
                #To open a second file
                with open(in_dir2 + "/" + wth_file1, "r") as wth2:
                    data2 = [line for line in wth2.readlines() if line.strip()][4:]

                #To open the copied file and append the second file
                with open(out_dir + "/" + wth_file1, "r+") as wth1:
                    data1 = [line for line in wth1.readlines() if line.strip()]
                    wth1.writelines(data2)
            else:
                print("The file ", wth_file1, " will not be updated.")

def update_wth(in_file, in_dir, out_dir):
    s1 = datetime.now()

    os.chdir(in_dir)
    tempdir = os.path.dirname(in_file) + '/temp'
    if os.path.exists(tempdir):
        shutil.rmtree(tempdir)
        os.mkdir(tempdir)
    else:
        os.mkdir(tempdir)

    outdir_hist = tempdir + '/historical'
    #Select files from historical dataset
    print('Selecting WTH files from repository...')
    sel_wthfiles(in_file, in_dir, outdir_hist)

    #get one file from the folder and get the latest date available.
    with open(os.listdir(outdir_hist)[1], "r") as f1:  # Reading wth files
        data = [line.split() for line in f1.readlines() if line.strip()]

    last_date = data[-1][0] #Get the latest date in file.
    dt_s = datetime.strptime(last_date, '%Y%j') + timedelta(days=1) #Add one day to the latest date.
    dt_st = dt_s.strftime('%Y%m%d') #The start date for the update.

    dt_e = datetime.today() - timedelta(days=4) #Four days before today because of SRAD latency.
    dt_ed = dt_e.strftime('%Y%m%d') #The end date in format for the update.

    #Getting corrected data
    out_cor_nc = tempdir + '/in_nc_cor'
    print('Getting corrected data from CHIRPS server...')
    get_correc_nc(dt_s, dt_e, out_cor_nc)

    #Run chirps for corrected data
    outdir_prec = tempdir + '/prec_pkl'
    print('Processing CHIRPS data...')
    chirps2(in_file, out_cor_nc, outdir_prec + '/prec_corr.pkl')

    #Getting the latest day available in prec corrected data.
    df = joblib.load(outdir_prec + '/prec_corr.pkl')
    lastday_corr = df.columns[-1]
    dt_s_p = datetime.strptime(lastday_corr, '%Y%j') + timedelta(days=1)

    #Getting preliminary data
    out_pre_nc = tempdir + '/in_nc_pre'
    print('Getting preliminary data from CHIRPS server...')
    get_prelim_nc(dt_s_p, dt_e, out_pre_nc)
    print('CHIRPS netCDF files in disk.')

    #Run chirps for preliminary data
    chirps2(in_file, out_pre_nc, outdir_prec + '/prec_prelim.pkl')

    #Merging corrected and preliminary precipitation data.
    precpkl(outdir_prec)
    print('CHIRPS processing data are complete.')

    # Getting NASA POWER data for the update period
    nasa_outdir = tempdir + '/nasap'
    print('Getting NASA POWER data...')
    nasa(in_file, dt_st, dt_ed, nasa_outdir)

    #Fusing NASA POWER and CHIRPS with QC on SRAD.
    update_dir = tempdir + '/update'
    print('Building the WTH files...')
    nasachirps(in_file, nasa_outdir, outdir_prec + '/prec.pkl', update_dir)

    #Merging historical with latest data.
    mergeWTH(outdir_hist, update_dir, out_dir)

    e1 = datetime.now()
    print("Time of execution for the update is: ", str(e1-s1))
