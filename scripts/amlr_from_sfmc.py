#!/usr/bin/env python

import os
import subprocess
import sys
import logging
import argparse


def access_secret_version(project_id, secret_id, version_id = 'latest'):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Import the Secret Manager client library.
    from google.cloud import secretmanager

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
        print("Data corruption detected.")
        return response

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8")
    # print("Plaintext: {}".format(payload))



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
    bucket_path = args.bucket_path
    sfmc_path = args.sfmc_path
    # cache_path = args.cache_path
    # processDbds_file = args.processDbds_file
    # cac2lower_file = args.cac2lower_file
    logging.info('Pulling files from SFMC for deployment {:}'.format(deployment))


    #--------------------------------------------
    # Checks
    deployment_split = deployment.split('-')
    if len(deployment_split[1]) != 8:
        logging.error('The deployment must be the glider name followed by the deployment date')
        return
    else:
        glider = deployment_split[0]
        year = deployment_split[1][0:4]

    if not os.path.isdir(bucket_path):
        logging.error('bucket_path ({:}) does not exist'.format(bucket_path))
        return

    if not os.path.isdir(sfmc_path):
        logging.error('sfmc_path ({:}) does not exist'.format(sfmc_path))
        return


    #--------------------------------------------
    # Create sfmc directory structure, if needed
    sfmc_depl_path = f'sfmc-{deployment}'
    if not os.path.isdir(os.path.join(sfmc_path, sfmc_depl_path)):
        logging.info('Making sfmc deployment directory at {:}'.format(sfmc_depl_path))
        os.mkdir(sfmc_depl_path)
        # os.mkdir(os.path.join(sfmc_depl_path, 'cache'))
        # os.mkdir(os.path.join(sfmc_depl_path, 'stbd'))
        # os.mkdir(os.path.join(sfmc_depl_path, 'ad2'))

    # Todo: create .sfmcpass.txt file, if necessary


    #--------------------------------------------
    # rsync with SFMC, and send files to their places in the bucket
    # access_secret_version('ggn-nmfs-usamlr-dev-7b99', 'sfmc-swoodman')
    sfmc_server_path = os.path.join('/var/opt/sfmc-dockserver/stations/noaa/gliders', glider, 'from-glider')
    sfmc_server = 'swoodman@sfmc.webbresearch.com:' + sfmc_server_path
    run_out = subprocess.run(['rsync', sfmc_server, sfmc_depl_path])

    # todo:
    # sshpass -p $(cat ~/.sfmcpass.txt) rsync swoodman@sfmc.webbresearch.com:/var/opt/sfmc-dockserver/stations/noaa/gliders/amlr03/from-glider/* tmp-sfmc

    # find sfmc_depl_path -iname *.cac | mv sfmc_depl_path/cache
    # find sfmc_depl_path -iname *.[st]bd | mv sfmc_depl_path/stbd
    # find sfmc_depl_path -iname *.ad2 | mv sfmc_depl_path/ad2

    # run_out_cache = subprocess.run('gsutil', '-m', 'rsync', os.path.join(sfmc_depl_path, '*.[Cc][Aa][Cc]'), todo)
    # run_out_stbd = subprocess.run('gsutil', '-m', 'rsync', os.path.join(sfmc_depl_path, '*.[st]bd'), todo)


    #--------------------------------------------
    # S




    return 0



if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('deployment', 
        type=str,
        help='Deployment name, eg amlr03-20220425')

    arg_parser.add_argument('bucket_path', 
        type=str,
        help='Path to the glider data bucket')

    arg_parser.add_argument('--sfmcpath', 
        type=str,
        dest='sfmc_path', 
        help='Path to the glider data bucket', 
        default='/home/sam_woodman_noaa_gov/sfmc')

    # arg_parser.add_argument('ascii_path', type=str,
    #                         help='Path to write ascii (dba) files. If it does not exist, this path will be created')

    # arg_parser.add_argument('cache_path', type=str,
    #                         help='Location of cache files')

    # arg_parser.add_argument('processDbds_file', type=str,
    #                         help='Path to processDbds shell script')

    # arg_parser.add_argument('--cac2lower_file', type=str,
    #                         default = '/opt/slocum/bin2ascii/cac2lower.sh', 
    #                         help='Location of cache files')

    arg_parser.add_argument('-l', '--loglevel',
        type=str,
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
