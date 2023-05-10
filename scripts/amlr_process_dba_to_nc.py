#!/usr/bin/env python

import os
import sys
import logging
import argparse

from amlrgliders.utils import amlr_year_path 
import amlrgliders.process as amlrp


def main(args):
    """
    Process raw AMLR glider data and write data to parquet and nc files.

    This script depends requires the directory structure specified in the 
    AMLR glider data management readme:
    https://docs.google.com/document/d/1X5DB4rQRBhBqnFdAAY_5Eyh_yPjG3UZ43Ga7ZGWcSsg
    
    Note the data written to the tmp files has not 
    removed 1970-01-01 timestamps, made column names lowercase, 
    or added inteprolated variables. 

    Returns the gdm object from amlr_gdm. 
    """

    #--------------------------------------------
    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)
    # TODO: write ^ logs to output file
    
    deployment = args.deployment
    project = args.project
    mode = args.mode
    deployments_path = args.deployments_path

    numcores = args.numcores
    chunksize = args.chunksize

    loadfromtmp = args.loadfromtmp
    write_trajectory = args.write_trajectory
    write_ngdac = args.write_ngdac
    
    write_acoustics = args.write_acoustics
    write_imagery = args.write_imagery
    imagery_path = args.imagery_path


    #--------------------------------------------
    # Checks and make glider_path variables

    prj_list = ['FREEBYRD', 'REFOCUS', 'SANDIEGO']    
    if not os.path.isdir(deployments_path):
        logging.error(f'deployments_path ({deployments_path}) does not exist')
        return
    else:
        dir_expected = prj_list + ['cache']
        if not all(x in os.listdir(deployments_path) for x in dir_expected):
            logging.error(f"The expected folders ({', '.join(dir_expected)}) " + 
                f'were not found in the provided directory ({deployments_path}). ' + 
                'Did you provide the right path via deployments_path?')
            return 

    deployment_split = deployment.split('-')
    deployment_mode = f'{deployment}-{mode}'
    year = amlr_year_path(project, deployment_split)

    glider_path = os.path.join(
        deployments_path, project, year, deployment, 'glider'
    )
    # glider_path = os.path.join(deployment_curr_path, 'glider')   


    #--------------------------------------------
    # Create gdm object  
    logging.info(f'Creating gdm object')
    gdm = amlrp.amlr_gdm(
        deployment, project, mode, glider_path, numcores, chunksize, loadfromtmp
    )

    if gdm is None:
        logging.error('gdm processing failed and processing will be aborted')
        return


    #--------------------------------------------
    # Do various additional processing steps

    # Convert to time series, and write trajectory data to nc file
    if write_trajectory:
        amlrp.amlr_write_trajectory(gdm, deployment_mode, glider_path)

    # Write individual (profile) nc files
    if write_ngdac:
        amlrp.amlr_write_ngdac(gdm, deployment_mode, glider_path)


    # Write acoustics files
    if write_acoustics: 
        if mode == 'rt':
            logging.warning('You are creating acoustic data files ' + 
                'using real-time data. ' + 
                'This may result in inaccurate acoustic file metadata')
        amlrp.amlr_acoustics(gdm, glider_path, deployment, mode)

    # Write imagery metadata file
    if write_imagery:
        if mode == 'rt':
            logging.warning('You are creating imagery file metadata ' + 
                'using real-time data. ' + 
                'This may result in inaccurate imagery file metadata')
        amlrp.amlr_imagery(gdm, glider_path, deployment, imagery_path)
        
    # All done
    logging.info(f'Glider data processing complete for {deployment_mode}')
    return gdm



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

    arg_parser.add_argument('--numcores',
        type=int,
        help='Number of cores to use when processing. ' + 
            'If greater than 1, parallel processing via mp.Pool.map will ' + 
            'be used for load_slocum_dbas and ' + 
            '(todo) writing individual (profile) nc files. ' +
            'This argument must be between 1 and mp.cpu_count(). ' + 
            'If 0 (the default), all possible cores will be used',
        default=0)
    
    arg_parser.add_argument('--chunksize',
        type=int,
        help='Number of files from which to concatenate dba data ' + 
            'before writing to temporary files to avoid ' + 
            'pandas.concat memory issues. ' + 
            'In other words, data frames of dba data extracted from ' + 
            'chunksize files will be concatenated and ' + 
            'written to temporary parquet files.',
        default=50)

    arg_parser.add_argument('--loadfromtmp',
        help='flag; indicates gdm object should be loaded from ' + 
            'parquet files in glider/data/tmp directory',
        action='store_true')

    arg_parser.add_argument('--write_trajectory',
        help='flag; indicates if trajectory nc file should be written',
        action='store_true')

    arg_parser.add_argument('--write_ngdac',
        help='flag; indicates if ngdac nc files should be written',
        action='store_true')

    arg_parser.add_argument('--write_acoustics',
        help='flag; indicates if acoustic files should be written',
        action='store_true')

    arg_parser.add_argument('--write_imagery',
        help='flag; indicates if imagery metadata csv file should be written',
        action='store_true')

    arg_parser.add_argument('--imagery_path',
        type=str,
        help='Path to imagery bucket',
        default='')

    arg_parser.add_argument('-l', '--loglevel',
        type=str,
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
