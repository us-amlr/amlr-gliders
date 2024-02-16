#!/usr/bin/env python

import os
import sys
import argparse
import logging
from subprocess import run

from amlrgliders.utils import amlr_year_path #, amlr_logger


def main(args):
    """
    Wrapper around cac2lower.sh and processDbds.sh; 
    Makes cac files lowercase and creates dbas;
    Requires Linux system with amlr-gliders cloned to /opt/;
    Returns 0
    """

    #--------------------------------------------
    # Set up logger and args variables
    # logger = amlr_logger(args.logfile, args.loglevel, 'amlr_binary_to_dba')
    loglevel = args.loglevel
    log_level = getattr(logging, loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)
 

    deployment = args.deployment
    project = args.project
    mode = args.mode
    deployments_path = args.deployments_path

    processDbds_file = args.processDbds_file
    cac2lower_file = args.cac2lower_file    
    linuxbin_path = args.linuxbin_path    

    # Checks
    if not os.path.isdir(deployments_path):
        logging.error(f'deployments_path ({deployments_path}) does not exist')
        return

    if not os.path.isfile(processDbds_file):
        logging.error(f'processDbds_file ({processDbds_file}) does not exist')
        return

    if not os.path.isfile(cac2lower_file):
        logging.error(f'cac2lower_file ({cac2lower_file}) does not exist')
        return

    if not os.path.isdir(linuxbin_path):
        logging.error(f'linuxbin_path ({linuxbin_path}) does not exist')
        return


    if not (mode in ['delayed', 'rt']):
        logging.error("mode must be either 'delayed' or 'rt'")
        return

    deployment_split = deployment.split('-')
    if len(deployment_split[1]) != 8:
        logging.error("The deployment string format must be 'glider-YYYYmmdd', " + 
            "eg amlr03-20220101")
        return

    #--------------------------------------------
    # Set/check/create file paths for processDbds script
    logging.info(f'Writing dba files for deployment {deployment}, mode {mode}')
    glider = deployment_split[0]
    year = amlr_year_path(project, deployment_split)

    glider_depl_path = os.path.join(deployments_path, project, year, deployment)
    # glider_data_in = os.path.join(glider_depl_path, 'glider', 'data', 'in')
    glider_data_in = os.path.join(glider_depl_path, 'data')
    
    binary_path = os.path.join(glider_data_in, 'binary', mode)
    ascii_path = os.path.join(glider_data_in, 'ascii', mode)
    cache_path = os.path.join(deployments_path, 'cache')
    scripts_path = os.path.join(glider_depl_path, 'scripts')
    processDbds_out_file = f'{deployment}_{mode}_processDbds_out.txt'

    logging.debug(f'processDbds file: {processDbds_file}')
    logging.debug(f'Cache path: {cache_path}')
    logging.debug(f'linux-bin path: {linuxbin_path}')
    logging.debug(f'Binary path: {binary_path}')
    logging.debug(f'Ascii path: {ascii_path}')
    logging.debug(f'Scripts path: {scripts_path}')
    logging.debug(f'processDbds_out file: {processDbds_out_file}')

    if not os.path.isdir(cache_path):
        logging.error(f'cache_path ({cache_path}) does not exist')
        return

    if not os.path.isdir(binary_path):
        logging.error(f'binary_path ({binary_path}) does not exist')
        return
    
    if not os.path.isdir(ascii_path):
        logging.info(f'Making path at: {ascii_path}')
        os.makedirs(ascii_path)

    if not os.path.isdir(scripts_path):
        logging.info(f'Making path at: {scripts_path}')
        os.makedirs(scripts_path)

    #--------------------------------------------
    # Make sure cache files are lowercase
    files_list = os.listdir(cache_path)
    files_list_CAC = list(filter(lambda i: i.endswith(".CAC"), files_list))

    if len(files_list_CAC) > 0:
        logging.info(f'{len(files_list_CAC)} .CAC files will be renamed')
        run_out = run([cac2lower_file, os.path.join(cache_path, "*")], 
            capture_output=True, text=True)

        if run_out.returncode != 0:
            logging.error(f'Error running `{cac2lower_file}`')
            logging.error(f'ARGS:\n{run_out.args}')
            logging.error(f'STDERR:\n{run_out.stderr}')
            return
        else:
            logging.info(f'Successfully completed run of `{cac2lower_file}`')
            logging.debug(f'ARGS:\n{run_out.args}')
            logging.debug(f'STDOUT:\n{run_out.stdout}')

        # Make sure that all .CAC files have corresponding .cac files before deleting
        delete_ok = True
        files_list_new = os.listdir(cache_path)

        for i in files_list_CAC:
            if i.lower() not in files_list_new:
                delete_ok = False

        if delete_ok: 
            run_out = run(["find", cache_path, '-name', '*.CAC', '-delete'])
            if run_out.returncode != 0:
                logging.error(f'Error running `find {cache_path} -name *.CAC -delete`')
                return

            logging.info(f"{len(files_list_CAC)} uppercase .CAC files were deleted")
        else:
            logging.warn("Not all '.CAC' files have a corresponding '.cac' file, " + 
                "and thus the .CAC files were not deleted. " + 
                "Please inspect by hand.")

            del run_out

    else:
        logging.info('There are no .CAC files to rename')


    # Make dba files
    logging.info(f'Running processDbds script and writing dba files to {ascii_path}')
    run_out = run([processDbds_file, "-c", cache_path, "-e", linuxbin_path, 
                   binary_path, ascii_path], 
                capture_output=True, text=True)    
    if run_out.returncode != 0:
        logging.error(f'Error running `{processDbds_file}`')
        logging.error(f'ARGS:\n{run_out.args}')
        logging.error(f'STDERR:\n{run_out.stderr}')
        return
    else:
        logging.info(f'Successfully completed run of `{processDbds_file}`')
        logging.debug(f'ARGS:\n{run_out.args}')
        logging.debug(f'STDOUT:\n{run_out.stdout}')

        # TODO: write to log directory, if applicable
        # fileout_path = os.path.join(scripts_path, processDbds_out_file)
        # logging.info(f'Writing `{processDbds_file}` output to {fileout_path}')
        # fileout = open(fileout_path, 'w')
        # fileout.write(f'ARGS PASSED TO processDbds SCRIPT:\n{run_out.args}\n\n\n')
        # fileout.write(f'STDOUT:\n{run_out.stdout}')
        # fileout.close()


    #--------------------------------------------
    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__, 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

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

    arg_parser.add_argument('--processDbds_file', 
        type=str,
        help='Path to processDbds shell script', 
        default = '/opt/amlr-gliders/resources/slocum/processDbds-usamlr.sh')

    arg_parser.add_argument('--cac2lower_file', 
        type=str, 
        help='Path to cac2lower shell script',
        default = '/opt/amlr-gliders/resources/slocum/cac2lower.sh')

    arg_parser.add_argument('--linuxbin_path', 
        type=str, 
        help='Path to linux-bin directory',
        default = '/opt/amlr-gliders/resources/slocum/linux-bin_8_6')

    arg_parser.add_argument('-l', '--loglevel',
        type=str,
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')
    
    arg_parser.add_argument('--logfile',
        type=str,
        help='File to which to write logs',
        default='')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
