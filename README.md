# nasapchirps_dssat - Version 2
Application to download weather data from the NASA POWER API version 2 and merge with CHIRPS dataset to produce WTH files. WTH files are the native format input for DSSAT.

There are two modes of using this tool. 

“get” is to fetch weather data based on a set of georeferenced points and a range of dates.

 “update” is to fetch weather data based on a directory of WTH files that the user wants to update up to the most recent available.

Inputs for “get” option:

in_file: A CSV file (.CSV) with the following columns: "ID", "Latitude", "Longitude", "nasapid", "LatNP", and "LonNP". If you want to use our 5-arc minute global grid (shapefile and CSV) file, please contact us. 

startDate: Start date with format YYYYMMDD (e.g. 19841224)

endDate: End date with format YYYYMMDD (e.g. 19841231)

out_dir: Path of output directory for the new WTH files.

Inputs for “update” option:

in_file: A CSV file (.CSV) with the following columns: "ID", "Latitude", "Longitude", "nasapid", "LatNP", and "LonNP". If you want to use our 5-arc minute global grid (shapefile and CSV) file, please contact us.

in_dir: Path directory of current WTH files to update.

out_dir: Path of output directory for the new WTH files.

How to run: Application is tested on Python 3.8.5 version and Linux environment.

python nasapchirps_dssat {get, update} argument1, argument2, …

python get in_file, startDate, endDate, out_dir

python update in_file, in_dir, out_dir

Note: For running on Windows OS systems, you may need to replace “/” by “\\” in the scripts.
