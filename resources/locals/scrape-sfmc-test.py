import os
import sys
import stat
import logging
import argparse
from subprocess import run

from amlrgliders.utils import amlr_year_path, find_extensions
from amlrgliders.scrape_sfmc import access_secret_version, rt_files_mgmt


deployment = 'amlr03-20230620'
project = 'SANDIEGO'

sfmc_path = os.path.join("C:/Users", "sam.woodman", "Downloads", "var-sfmc")
sfmc_pwd_file_name = '.sfmcpwd.txt'
gcpproject_id = 'ggn-nmfs-usamlr-dev-7b99'
bucket = 'amlr-gliders-deployments-dev'
secret_id = 'sfmc-swoodman'

sfmc_local_path = os.path.join(sfmc_path, f'sfmc-{deployment}')
name_cac  = 'cac'
name_stbd = 'stbd'
name_ad2  = 'ad2'
# name_cam  = os.path.join(sfmc_local_path, 'cam')

deployment_split = deployment.split('-')
glider = deployment_split[0]
year = amlr_year_path(project, deployment_split)

    
if not os.path.isdir(sfmc_local_path):
    logging.info('Making sfmc deployment directory and subdirectories ' + 
        f'at {sfmc_local_path}')
    os.mkdir(sfmc_local_path)
    os.mkdir(os.path.join(sfmc_local_path, name_cac))
    os.mkdir(os.path.join(sfmc_local_path, name_stbd))
    os.mkdir(os.path.join(sfmc_local_path, name_ad2))
    # os.mkdir(os.path.join(sfmc_local_path, name_cam))
    
sfmc_pwd_file = os.path.join(sfmc_local_path, sfmc_pwd_file_name)
if not os.path.isfile(sfmc_pwd_file):
    logging.info('Writing SFMC ssh pwd to file')
    file = open(sfmc_pwd_file, 'w+')
    file.write(access_secret_version(gcpproject_id, secret_id))
    file.close()
    os.chmod(sfmc_pwd_file, stat.S_IREAD)
    
sfmc_noaa = '/var/opt/sfmc-dockserver/stations/noaa/gliders'
sfmc_server_path = os.path.join(sfmc_noaa, glider, 'from-glider', "*")
sfmc_server = f'swoodman@sfmc.webbresearch.com:{sfmc_server_path}'
retcode = run(['sshpass', '-p', access_secret_version(gcpproject_id, secret_id), 
               'rsync', sfmc_server, sfmc_local_path], 
    capture_output=True)
  