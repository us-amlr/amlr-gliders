#!/usr/bin/env python

import os
import subprocess
import sys
import logging
import argparse

def access_secret_version(project_id, secret_id, version_id):
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
    print("Plaintext: {}".format(payload))


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

    # binary_path = args.binary_path
    # ascii_path = args.ascii_path
    # cache_path = args.cache_path
    # processDbds_file = args.processDbds_file
    # cac2lower_file = args.cac2lower_file


    #--------------------------------------------
    # Checks



if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # arg_parser.add_argument('binary_path', type=str,
    #                         help='Location of binary ([dest]bd) files')

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
