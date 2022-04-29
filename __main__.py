#!/usr/bin/env python
#######################################
#Script developed by Oscar Castillo
#University of Florida
#2022
#Contact email: ocastilloromero@ufl.edu
#######################################

import sys
import argparse
from dssat_wth import dssat_wth
from update_wth import update_wth

def main():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest='command')
    getwth = subparser.add_parser('get')
    updatewth = subparser.add_parser('update')

    getwth.add_argument('in_file', type=str, help='CSV file with the points required. It must contain ID, Latitude, Longitude, nasapid, LatNP, LonNP columns.')
    getwth.add_argument('startDate', type=int, help='Start date with format YYYYMMDD (e.g. 19841224)')
    getwth.add_argument('endDate', type=int, help='End date with format YYYYMMDD (e.g. 19841231)')
    getwth.add_argument('out_dir', type=str, help='Path of output directory for the new WTH files.')

    updatewth.add_argument('in_file', type=str, help='CSV file with the points required. It must contain ID, Latitude, Longitude, nasapid, LatNP, LonNP columns.')
    updatewth.add_argument('in_dir', type=str, help='Path directory of current WTH files to update.')
    updatewth.add_argument('out_dir', type=str, help='Path of output directory for the new WTH files.')

    args = parser.parse_args()

    if args.command == 'get':
        dssat_wth(args.in_file, args.startDate, args.endDate, args.out_dir)
    elif args.command == 'update':
        update_wth(args.in_file, args.in_dir, args.out_dir)

if __name__ == "__main__":
    sys.exit(main())