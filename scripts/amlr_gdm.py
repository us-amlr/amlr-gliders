#!/usr/bin/env python

import os
import sys
import logging
import argparse

import multiprocessing as mp
import pandas as pd



def main(args):
    """
    Process raw AMLR glider data and write data to parquet and nc files.

    This script depends requires the directory structure specified in the 
    AMLR glider data management readme:
    https://docs.google.com/document/d/1X5DB4rQRBhBqnFdAAY_5Eyh_yPjG3UZ43Ga7ZGWcSsg

    Returns a gdm object. Note the gdm object in the tmp file has not 
    removed 1970-01-01 timestamps or made column names lowercase. 
    """

    #--------------------------------------------
    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)
    
    deployment = args.deployment
    project = args.project
    mode = args.mode
    deployments_path = args.deployments_path


    # Check mode and deployment and set related variables
    if not (mode in ['delayed', 'rt']):
        logging.error("mode must be either 'delayed' or 'rt'")
        return
    else:
        if mode == 'delayed':
            binary_type = 'debd'
        else: 
            binary_type = 'stbd'


    deployment_split = deployment.split('-')
    if len(deployment_split[1]) != 8:
        logging.error("The deployment string format must be 'glider-YYYYmmdd', eg amlr03-20220101")
        return

    else:
        logging.info(f'Writing dba files for deployment {deployment}, mode {mode}')
        glider = deployment_split[0]
        year = deployment_split[1][0:4]
        # glider_data_in = os.path.join(deployments_path, project, year, deployment, 
        #     'glider', 'data', 'in')
        # binary_path = os.path.join(glider_data_in, 'binary', binary_type)
        # ascii_path = os.path.join(glider_data_in, 'ascii', binary_type)
        # cache_path = os.path.join(deployments_path, 'cache')

        # logging.debug(f'Binary path: {binary_path}')
        # logging.debug(f'Ascii path: {ascii_path}')
        # logging.debug(f'Cache path: {cache_path}')


    # Append gdm path, and import functions
    gdm_path = args.gdm_path
    if not os.path.isdir(gdm_path):
        logging.error(f'gdm_path ({gdm_path}) does not exist')
        return
    else:
        sys.path.append(gdm_path)
        from gdm import GliderDataModel
        from gdm.gliders.slocum import load_slocum_dba #, get_dbas



    #--------------------------------------------
    logging.info('all good')
    # return gdm



if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__, 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter, 
        allow_abbrev=False)

    arg_parser.add_argument('deployment', 
        type=str,
        help='Deployment name, eg amlr03-20220425')

    arg_parser.add_argument('project', 
        type=str,
        help='Glider project name', 
        choices=['FREEBYRD', 'REFOCUS', 'SANDIEGO'])

    arg_parser.add_argument('mode', 
        type=str,
        help="Specify which binary files will be converted to dbas. " + 
            "'delayed' means [de]bd files will be converted, " + 
            "and 'rt' means [st]bd files will be converted", 
        choices=['delayed', 'rt'])

    arg_parser.add_argument('deployments_path', 
        type=str,
        help='Path to glider deployments directory. ' + 
            'In GCP, this will be the mounted bucket path')

    
    arg_parser.add_argument('--gdm_path', 
        type=str,
        help='Path to gdm module', 
        default='/opt/gdm')

    arg_parser.add_argument('--numcores',
        type=int,
        help='Number of cores to use when processing. ' + 
            'If greater than 1, parallel processing via mp.Pool.map will ' + 
            'be used for load_slocum_dbas and ' + 
            '(todo) writing individual (profile) nc files. ' +
            'This argument must be between 1 and mp.cpu_count()',
        default=1)

    arg_parser.add_argument('--load_from_tmp',
        help='boolean; indicates gdm object should be loaded from ' + 
            'parquet files in glider/data/tmp directory',
        action='store_false')

    arg_parser.add_argument('--remove_19700101',
        help='boolean; indicates if data with the timestamp 1970-01-01 '  + 
            'should be removed (before writing to pkl file). ' + 
            'Will be ignored if load_from_tmp is True. ' + 
            "Removing these timestamps is for situations when there is a " + 
            "'Not enough timestamps for yo interpolation' warning",
        action='store_true')

    arg_parser.add_argument('--write_trajectory',
        help='boolean; indicates if trajectory nc file should be written',
        action='store_false')

    arg_parser.add_argument('--write_ngdac',
        help='boolean; indicates if ngdac nc files should be written',
        action='store_false')

    arg_parser.add_argument('-l', '--loglevel',
        type=str,
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
