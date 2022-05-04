#!/usr/bin/env python

import os
import stat
from subprocess import Popen, run, PIPE
import sys
import logging
import argparse
import pathlib


def access_secret_version(project_id, secret_id, version_id = 'latest'):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Import the Secret Manager client library.
    from google.cloud import secretmanager
    import google_crc32c

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        logging.error("Data corruption detected.")
        return response

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    # payload = response.payload.data.decode("UTF-8")
    # print("Plaintext: {}".format(payload))

    return response.payload.data.decode("UTF-8")



# From https://stackoverflow.com/questions/45256250
def find_extensions(dir_path): #,  excluded = ['', '.txt', '.lnk']):
    extensions = set()
    for _, _, files in os.walk(dir_path):   
        for f in files:
            extensions.add(pathlib.Path(f).suffix.lower())
            # ext = Path(f).suffix.lower()
            # if not ext in excluded:
            #     extensions.add(ext)
    return extensions


  
# https://www.geeksforgeeks.org/python-filter-list-of-strings-based-on-the-substring-list/
def Filter(string, substr):
    import re
    return [str for str in string 
    if re.match(r'[^\d]+|^', str).group(0) in substr]



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
        year = deployment_split[1][0:4]

    if not os.path.isdir(sfmc_path):
        logging.error(f'sfmc_path ({sfmc_path}) does not exist')
        return


    #--------------------------------------------
    # Create sfmc directory structure, if needed
    sfmc_local_path = os.path.join(sfmc_path, f'sfmc-{deployment}')
    # sfmc_local_cache = 'cache'
    sfmc_local_stbd = 'stbd'
    sfmc_local_ad2 = 'ad2'

    if not os.path.isdir(sfmc_local_path):
        logging.info(f'Making sfmc deployment directory at {sfmc_local_path}')
        os.mkdir(sfmc_local_path)
        # os.mkdir(os.path.join(sfmc_local_path, sfmc_local_cache))
        os.mkdir(os.path.join(sfmc_local_path, sfmc_local_stbd))
        os.mkdir(os.path.join(sfmc_local_path, sfmc_local_ad2))

    if not os.path.isfile(sfmc_pwd_file):
        logging.info('Writing SFMC ssh pwd to file')
        file = open(sfmc_pwd_file, 'w+')
        file.write(access_secret_version(gcpproject_id, secret_id))
        file.close
        os.chmod(sfmc_pwd_file, stat.S_IREAD)


    #--------------------------------------------
    # rsync with SFMC, and send files to their places in the bucket
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


    # Check for unexpected file extensions
    sfmc_file_ext = find_extensions(sfmc_local_path)
    file_ext_expected = {".cac", ".sbd", ".tbd", ".ad2"}
    file_ext_weird = sfmc_file_ext.difference(file_ext_expected)
    if len(file_ext_weird) > 0:
        x = os.listdir(sfmc_local_path)
        logging.warning(f'File with unexpected extensions ({file_ext_weird}) were downloaded from the SFMC')
        logging.warning(f'File list: TODO')
    # TODO: Print warning message if there are other file extensions are in here


    # Copy files to subfolders, and rsync with bucket
    # https://docs.python.org/3/library/subprocess.html#replacing-bin-sh-shell-command-substitution
    # retcode_cp_cache = call(f"find {sfmc_local_path} -iname *.cac | xargs cp -t {os.path.join(sfmc_local_path, sfmc_local_cache)}", shell=True)
    # retcode_cp_stbd  = call(f"find {sfmc_local_path} -iname *.[st]bd | xargs cp -t {os.path.join(sfmc_local_path, sfmc_local_stbd)}", shell=True)
    # retcode_cp_ad2   = call(f"find {sfmc_local_path} -iname *.ad2 | xargs cp -t {os.path.join(sfmc_local_path, sfmc_local_ad2)}", shell=True)

    logging.info('Starting file management')
    bucket_data_in = f'gs://{bucket}/{project}/{year}/{deployment}/glider/data/in'
    logging.debug(f"GCP bucket data/in folder: {bucket_data_in}")

    if ('.cac' in sfmc_file_ext):
        retcode_cache = run(['gsutil', '-m', 'cp', 
            os.path.join(sfmc_local_path, '*.[Cc][Aa][Cc]'), 
            f'gs://{bucket}/cache'], 
        capture_output = True)

        if retcode_cache.returncode != 0:
            logging.error('Error copying cache files to bucket')
            logging.error(f'Args: {retcode_cache.args}')
            logging.error(f'stderr: {retcode_cache.stderr}')
            return
        else:
            logging.info(f'Successfully copied cache files to bucket')

    else: 
        logging.info('No cache files to copy')


    if ('.sbd' in sfmc_file_ext) or ('.tbd' in sfmc_file_ext):
        logging.info('Copying [st]bd files into their subdirectory')
        p1 = Popen(['find', sfmc_local_path, '-iname', '*.[st]bd'], stdout=PIPE)
        p2 = Popen(["xargs", "cp", "-t", os.path.join(sfmc_local_path, sfmc_local_stbd)], 
            stdin=p1.stdout, stdout=PIPE)
        p1.stdout.close()

        retcode_stbd = run(['gsutil', '-m', 'rsync', 
            os.path.join(sfmc_local_path, sfmc_local_stbd), 
            f'{bucket_data_in}/binary/{sfmc_local_stbd}'], 
        capture_output = True)

        if retcode_stbd.returncode != 0:
            logging.error('Error rsyncing stbd files to bucket')
            logging.error(f'Args: {retcode_stbd.args}')
            logging.error(f'stderr: {retcode_stbd.stderr}')
            return
        else:
            logging.info(f'Successfully rsyncd [st]bd files to bucket')

    else:
        logging.info('No [st]bd files to copy')


    if ('.ad2' in sfmc_file_ext):
        logging.info('Copying ad2 files into their subdirectory')
        p1 = Popen(['find', sfmc_local_path, '-iname', '*.ad2'], stdout=PIPE)
        p2 = Popen(["xargs", "cp", "-t", os.path.join(sfmc_local_path, sfmc_local_ad2)], 
            stdin=p1.stdout, stdout=PIPE)
        p1.stdout.close()

        retcode_ad2 = run(['gsutil', '-m', 'rsync', 
            os.path.join(sfmc_local_path, sfmc_local_ad2), 
            f'{bucket_data_in}/{sfmc_local_ad2}'], 
        capture_output = True)

        if retcode_ad2.returncode != 0:
            logging.error('Error rsyncing ad2 files to bukcet')
            logging.error(f'Args: {retcode_ad2.args}')
            logging.error(f'stderr: {retcode_ad2.stderr}')
            return
        else:
            logging.info(f'Successfully rsyncd ad2 files to bucket')

    else:
        logging.info('No ad2 files to copy')


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
        help='The SFMC directory on the local machine where the files from the SFMC will be copied', 
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

    arg_parser.add_argument('-l', '--loglevel',
        type=str,
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
