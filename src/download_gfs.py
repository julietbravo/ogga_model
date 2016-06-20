
# Copyright (c) 2013-2014 Bart van Stratum (bart@vanstratum.com)
# 
# This file is part of OLGA.
# 
# OLGA is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
# 
# OLGA is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with OLGA.  If not, see <http://www.gnu.org/licenses/>.
#

import numpy as np
import os
import sys
import time
import datetime
from multiprocessing import Process

try:
    from urllib.request import urlopen
    from urllib.request import urlretrieve
    PYTHON3 = True
except ImportError: 
    from urllib import urlopen
    from urllib import urlretrieve
    PYTHON3 = False

DEBUG = True

# Execute command
def execute(task):
    subprocess.call(task, shell=True, executable='/bin/bash')

# Print to stdout, and flush buffer
def printf(message):
    print(message)
    sys.stdout.flush() 

# Return time in hh:mm:ss formatted string
def get_time():
    now = datetime.datetime.now().time()
    return '{0:02d}:{1:02d}:{2:02d}'.format(now.hour, now.minute, now.second)

# Download a single GFS file
def download_gfs_file(file_name, remote_file, local_file):
    if(DEBUG): printf('processing {}...'.format(file_name))

    success = False
    while (success == False):
        # Test: put try around everything to catch weird exceptions
        try:
            # Check if file locally available and valid:
            if (os.path.isfile(local_file)): 
                if (DEBUG): printf('found {} local'.format(file_name))
                remote = urlopen(remote_file) # Check if same size as remote:
               
                # Check if file is available online (might have been removed),
                # and check if the remote and local file sizes match
                if (remote.code == 200):
                    if (PYTHON3):
                        size_remote = remote.getheader("Content-Length")[0]
                    else:
                        meta = remote.info()
                        size_remote = meta.getheaders("Content-Length")[0] 

                    local      = open(local_file, 'rb')
                    size_local = len(local.read())

                    if (int(size_remote) == int(size_local)):
                        if (DEBUG): printf('size {} remote/local match, success!'.format(file_name))
                        success = True
                    else:
                        if (DEBUG): printf('size {} remote/local differ, re-download... '.format(file_name))
                        success = False
                else:
                    if (DEBUG): printf('remote {} not available (probably old run), pass'.format(file_name))
                    success = True

            # If file not available local, check if available at server:
            if (success == False):
                check = urlopen(remote_file)
                if (check.code == 200):
                    # File is available, download! 
                    if(DEBUG): printf('file {} available at GFS server at {} -> downloading'.format(file_name, get_time()))
                    urlretrieve(remote_file, local_file)
                else:
                    # File not (yet) available, sleep a while and re-do the checks 
                    time.sleep(300)
        except:
            # Something weird happened. Sleep a bit, try again
            printf('weird exception on file {}: {}'.format(file_name, sys.exc_info()[0]))
            time.sleep(30)

# Download a bunch of GFS files
def download_gfs(year_run, month_run, day_run, cycle_run, fct_hour_start, fct_hour_end, fct_hour_dt):
    printf('Downloading GFS at {}'.format(get_time()))

    # Remote (GFS) and local directories
    remote_dir = 'http://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{0:04d}{1:02d}{2:02d}{3:02d}/'\
                      .format(year_run, month_run, day_run, cycle_run)
    local_dir  = 'gfs_data/{0:04d}_{1:02d}_{2:02d}_c{3:02d}/'\
                      .format(year_run, month_run, day_run, cycle_run)

    # Check if local directory exists, and if not, create it
    if not os.path.exists(local_dir):
        if(DEBUG): printf('Making directory {}'.format(local_dir))
        os.mkdir(local_dir)
   
    # Loop over all forecast times to download
    n_times = int((fct_hour_end - fct_hour_start) / fct_hour_dt + 1)
    download_processes = []
    for t in range(n_times):
        time = fct_hour_start + t * fct_hour_dt

        file_name   = 'gfs.t{0:02d}z.pgrb2.0p25.f{1:03d}'.format(cycle_run, time)
        remote_file = '{0:}{1:}'.format(remote_dir, file_name) 
        local_file  = '{0:}{1:}'.format(local_dir,  file_name)

        # Download files in parallel 
        download_processes.append(Process(target=download_gfs_file, args=(file_name, remote_file, local_file,)))
        download_processes[-1].start()
         
    # Join all processes 
    for download in download_processes:
        download.join() 

    printf('Finished GFS download at {}'.format(get_time()))

if __name__ == "__main__":
    download_gfs(2016, 6, 20, 0, 0, 24, 3)
