#!/usr/bin/env python

import os
import sys
import stat
import logging
import argparse
import pathlib
from subprocess import call, run

from amlrgliders.utils import amlr_year_path, path_check, find_extensions
from amlrgliders.scrape_sfmc import line_prepender, access_secret_version, rt_file_management


def main(args):
    """
    rsync files from sfmc, and send them to correct bucket directories;
    Returns 0
    """

    #--------------------------------------------
    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    deployment = args.deployment
    project = args.project

    sfmc_path = args.sfmc_path
    sfmc_pwd_file = args.sfmc_pwd_file
    gcpproject_id = args.gcpproject_id
    bucket = args.bucket
    secret_id = args.secret_id

    logging.info(f'Pulling files from SFMC for deployment {deployment}')


    #--------------------------------------------
    # Checks
    deployment_split = deployment.split('-')
    if len(deployment_split[1]) != 8:
        logging.error('The deployment must be the glider name followed by the deployment date')
        return
    else:
        glider = deployment_split[0]
        # year = deployment_split[1][0:4]
        # amlr_path = args.amlr_path
        # logging.info(f'Appending path ({amlr_path}) and importing year function')
        # sys.path.append(amlr_path)
        # from amlr import amlr_year_path
        year = amlr_year_path(project, deployment_split)


    path_check(sfmc_path)
    # if not os.path.isdir(sfmc_path):
    #     logging.error(f'sfmc_path ({sfmc_path}) does not exist')
    #     return


    #--------------------------------------------
    # Create sfmc directory structure, if needed
    sfmc_local_path = os.path.join(sfmc_path, f'sfmc-{deployment}')
    sfmc_local_cac  = os.path.join(sfmc_local_path, 'cac')
    sfmc_local_stbd = os.path.join(sfmc_local_path, 'stbd')
    sfmc_local_ad2  = os.path.join(sfmc_local_path, 'ad2')
    sfmc_local_cam  = os.path.join(sfmc_local_path, 'cam')

    if not os.path.isdir(sfmc_local_path):
        logging.info('Making sfmc deployment directory and subdirectories ' + 
            f'at {sfmc_local_path}')
        os.mkdir(sfmc_local_path)
        os.mkdir(sfmc_local_cac)
        os.mkdir(sfmc_local_stbd)
        os.mkdir(sfmc_local_ad2)
        os.mkdir(sfmc_local_cam)

    if not os.path.isfile(sfmc_pwd_file):
        logging.info('Writing SFMC ssh pwd to file')
        file = open(sfmc_pwd_file, 'w+')
        file.write(access_secret_version(gcpproject_id, secret_id))
        file.close
        os.chmod(sfmc_pwd_file, stat.S_IREAD)


    #--------------------------------------------
    # rsync with SFMC
    sfmc_server_path = os.path.join('/var/opt/sfmc-dockserver/stations/noaa/gliders', 
        glider, 'from-glider', "*")
    sfmc_server = f'swoodman@sfmc.webbresearch.com:{sfmc_server_path}'

    # retcode = run(['rsync', sfmc_server, sfmc_local_path])
    logging.info(f'Starting rsync with SFMC dockerver for {glider}')
    retcode = run(['sshpass', '-f', sfmc_pwd_file, 'rsync', sfmc_server, sfmc_local_path], 
        capture_output=True)
    logging.debug(retcode.args)

    if retcode.returncode != 0:
        logging.error('Error rsyncing with SFMC dockserver')
        logging.error(f'Args: {retcode.args}')
        logging.error(f'stderr: {retcode.stderr}')
        return
    else:
        logging.info(f'Successfully completed rsync with SFMC dockerver for {glider}')
        logging.debug(f'Args: {retcode.args}')
        logging.debug(f'stderr: {retcode.stdout}')


    # Check for unexpected file extensions
    sfmc_file_ext = find_extensions(sfmc_local_path)
    file_ext_expected = {".cac", ".CAC", ".sbd", ".tbd", ".ad2", ".cam"}
    file_ext_weird = sfmc_file_ext.difference(file_ext_expected)
    if len(file_ext_weird) > 0:
        x = os.listdir(sfmc_local_path)
        logging.warning(f'File with unexpected extensions ({file_ext_weird}) ' + 
            'were downloaded from the SFMC, ' + 
            'but will not be copied to the GCP bucket')
        # logging.warning(f'File list: TODO')


    #--------------------------------------------
    # Copy files to subfolders, and rsync with bucket
    # TODO: make 
    # https://docs.python.org/3/library/subprocess.html#replacing-bin-sh-shell-command-substitution

    logging.info('Starting file management')
    bucket_deployment = f'gs://{bucket}/{project}/{year}/{deployment}'
    bucket_stbd = os.path.join(bucket_deployment, 'glider', 'data', 'in', 'binary', 'rt')
    bucket_ad2 = os.path.join(bucket_deployment, 'sensors', 'nortek', 'data', 'in', 'rt')
    # bucket_cam = os.path.join(bucket_deployment, 'sensors', 'glidercam', 'data', 'in', 'rt')
    logging.debug(f"GCP bucket deployment folder: {bucket_deployment}")
    logging.debug(f"GCP bucket stbd folder: {bucket_stbd}")
    logging.debug(f"GCP bucket ad2 folder: {bucket_ad2}")

    ### cache files
    rt_file_management(sfmc_file_ext, '.cac', '*.[Cc][Aa][Cc]', 'cache', 
        sfmc_local_path, sfmc_local_cac, f'gs://{bucket}/cache')
    # if ('.cac' in sfmc_file_ext):
    #     retcode_cache = run(['gsutil', '-m', 'cp', 
    #             os.path.join(sfmc_local_path, '*.[Cc][Aa][Cc]'), 
    #             f'gs://{bucket}/cache'], 
    #         capture_output = True)
    #     if retcode_cache.returncode != 0:
    #         logging.error('Error copying cache files to bucket')
    #         logging.error(f'Args: {retcode_cache.args}')
    #         logging.error(f'stderr: {retcode_cache.stderr}')
    #         return
    #     else:
    #         logging.info(f'Successfully copied cache files to bucket')
    #         logging.debug(f'Args: {retcode_cache.args}')
    #         logging.debug(f'stderr: {retcode_cache.stdout}')
    # else: 
    #     logging.info('No cache files to copy')


    ### sbd/tbd files
    # TODO: add in support for compressed files
    # TODO: make function check for multiple ext values. a way to check in with regex?
    rt_file_management(sfmc_file_ext, 'todo', '*.[st]bd', 'cache', 
        sfmc_local_path, sfmc_local_stbd, bucket_stbd)
    # rt_file_management(sfmc_file_ext, 'todo', '*.[st]bd', 'cache', 
    #     sfmc_local_path, sfmc_local_stbd)

    # if ('.sbd' in sfmc_file_ext) or ('.tbd' in sfmc_file_ext):
    #     logging.info('Copying [st]bd files into their subdirectory')
    #     tmp = os.path.join(sfmc_local_path, '*.[st]bd')
    #     sfmc_local_stbd_path = os.path.join(sfmc_local_path, sfmc_local_stbd_whoops)
    #     retcode_tmp = call(f'rsync {tmp} {sfmc_local_stbd_path}', 
    #         shell = True)

    #     retcode_stbd = run(['gsutil', '-m', 'rsync', '-r', 
    #             sfmc_local_stbd_path, bucket_stbd], 
    #         capture_output = True)

    #     if retcode_stbd.returncode != 0:
    #         logging.error('Error rsyncing stbd files to bucket')
    #         logging.error(f'Args: {retcode_stbd.args}')
    #         logging.error(f'stderr: {retcode_stbd.stderr}')
    #         return
    #     else:
    #         logging.info(f'Successfully rsyncd [st]bd files to bucket')
    #         logging.debug(f'Args: {retcode_stbd.args}')
    #         logging.debug(f'stderr: {retcode_stbd.stdout}')

    # else:
    #     logging.info('No [st]bd files to copy')


    ### ad2 files
    rt_file_management(sfmc_file_ext, '.ad2', '*.ad2', 'ad2', 
        sfmc_local_path, sfmc_local_ad2, bucket_ad2)
    # if ('.ad2' in sfmc_file_ext):
    #     logging.info('Copying ad2 files into their subdirectory')
    #     tmp = os.path.join(sfmc_local_path, '*.ad2')
    #     sfmc_local_ad2_path = os.path.join(sfmc_local_path, sfmc_local_ad2)
    #     retcode_tmp = call(f'rsync {tmp} {sfmc_local_ad2_path}', 
    #         shell = True)

    #     retcode_ad2 = run(['gsutil', '-m', 'rsync', '-r', 
    #             sfmc_local_ad2_path,  bucket_ad2], 
    #         capture_output = True)

    #     if retcode_ad2.returncode != 0:
    #         logging.error('Error rsyncing ad2 files to bukcet')
    #         logging.error(f'Args: {retcode_ad2.args}')
    #         logging.error(f'stderr: {retcode_ad2.stderr}')
    #         return
    #     else:
    #         logging.info(f'Successfully rsyncd ad2 files to bucket')
    #         logging.debug(f'Args: {retcode_ad2.args}')
    #         logging.debug(f'stderr: {retcode_ad2.stdout}')

    # else:
    #     logging.info('No ad2 files to copy')


    ### cam files
    # TODO


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

    arg_parser.add_argument('--sfmcpath', 
        type=str,
        dest='sfmc_path', 
        help='The SFMC directory on the local machine ' + 
            'where the files from the SFMC will be copied', 
        default='/home/sam_woodman_noaa_gov/sfmc')

    arg_parser.add_argument('--sfmcpwd', 
        type=str,
        dest='sfmc_pwd_file', 
        help='The file that contains the SFMC password for the rsync', 
        default='/home/sam_woodman_noaa_gov/sfmc/.sfmcpwd.txt')

    arg_parser.add_argument('--gcpproject', 
        type=str,
        dest='gcpproject_id', 
        help='GCP project ID', 
        default='ggn-nmfs-usamlr-dev-7b99')

    arg_parser.add_argument('--gcpbucket', 
        type=str,
        dest='bucket', 
        help='GCP glider deployments bucket name', 
        default='amlr-gliders-deployments-dev')

    arg_parser.add_argument('--gcpsecret', 
        type=str,
        dest='secret_id', 
        help='GCP secret ID that contains the SFMC password for the rsync', 
        default='sfmc-swoodman')

    arg_parser.add_argument('--amlr_path', 
        type=str,
        help='Path to amlr module', 
        default='amlr-gliders/amlr-gliders')

    arg_parser.add_argument('-l', '--loglevel',
        type=str,
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
