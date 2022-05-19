#!/usr/bin/env python

import os
from subprocess import run
import sys
import logging
import argparse


def main(args):
    """
    Wrapper around cac2lower.sh and processDbds.sh; 
    Makes cac files lowercase and creates dbas;
    Requires Linux system with kerfoot/slocum cloned to /opt;
    Returns 0
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

    processDbds_file = args.processDbds_file
    cac2lower_file = args.cac2lower_file    


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
        glider_data_in = os.path.join(deployments_path, project, year, deployment, 
            'glider', 'data', 'in')
        binary_path = os.path.join(glider_data_in, 'binary', binary_type)
        ascii_path = os.path.join(glider_data_in, 'ascii', binary_type)
        cache_path = os.path.join(deployments_path, 'cache')

        logging.debug(f'Binary path: {binary_path}')
        logging.debug(f'Ascii path: {ascii_path}')
        logging.debug(f'Cache path: {cache_path}')




    #--------------------------------------------
    # Checks, and create files paths
    if not os.path.isdir(deployments_path):
        logging.error(f'deployments_path ({deployments_path}) does not exist')
        return

    if not os.path.isfile(processDbds_file):
        logging.error(f'processDbds_file ({processDbds_file}) does not exist')
        return

    if not os.path.isfile(cac2lower_file):
        logging.error(f'cac2lower_file ({cac2lower_file}) does not exist')
        return

    if not os.path.isdir(cache_path):
        logging.error(f'cache_path ({cache_path}) does not exist')
        return

    if not os.path.isdir(binary_path):
        logging.error(f'binary_path ({binary_path}) does not exist')
        return
    
    if not os.path.isdir(ascii_path):
        logging.info(f'Making path at: {ascii_path}')
        os.makedirs(ascii_path)




    #--------------------------------------------
    # Make sure cache files are lowercase
    files_list = os.listdir(cache_path)
    files_list_CAC = list(filter(lambda i: i.endswith(".CAC"), files_list))

    if len(files_list_CAC) > 0:
        logging.info(f'{len(files_list_CAC)} .CAC files will be renamed')
        run_out = run([cac2lower_file, os.path.join(cache_path, "*")])

        if run_out.returncode != 0:
            logging.error(f'Error running `{cac2lower_file} {os.path.join(cache_path, "*")}`')
            return

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
            logging.warn("Not all '.CAC' files have a corresponding '.cac' file, and thus the .CAC files were not deleted")

    else:
        logging.info('There are no .CAC files to rename')


    #--------------------------------------------
    # Make dba files
    logging.info(f'Running processDbds script and writing dba files to {ascii_path}')
    run_out = run([processDbds_file, "-c", cache_path, binary_path, ascii_path], 
        capture_output=True)    
    if run_out.returncode != 0:
        logging.error(f'Error running `{processDbds_file}`')
        logging.error(f'Args: {run_out.args}')
        logging.error(f'stderr: {run_out.stderr}')
        return
    else:
        logging.info(f'Successfully completed run of `{processDbds_file}`')
        logging.info(f'Args: {run_out.args}')
        logging.info(f'Args: {run_out.stdout}')


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

    arg_parser.add_argument('processDbds_file', 
        type=str,
        help='Path to processDbds shell script')

    arg_parser.add_argument('--cac2lower_file', 
        type=str, 
        help='Location of cache files',
        default = '/opt/slocum/bin2ascii/cac2lower.sh')

    arg_parser.add_argument('-l', '--loglevel',
        type=str,
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
