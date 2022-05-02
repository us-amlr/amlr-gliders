#!/usr/bin/env python

import os
import subprocess
import sys
import logging
import argparse


def main(args):
    """
    Wrapper around cac2lower.sh and processDbds.sh; makes cac files lowercase
    Requires Linux system with kerfoot/slocum cloned to /opt

    Returns none
    """

    #--------------------------------------------
    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    binary_path = args.binary_path
    ascii_path = args.ascii_path
    cache_path = args.cache_path
    processDbds_file = args.processDbds_file
    cac2lower_file = args.cac2lower_file


    #--------------------------------------------
    # Checks
    if not os.path.isfile(processDbds_file):
        logging.error('processDbds_file ({:}) does not exist'.format(processDbds_file))
        return

     if not os.path.isfile(cac2lower_file):
        logging.error('cac2lower_file ({:}) does not exist'.format(cac2lower_file))
        return

    if not os.path.isdir(cache_path):
        logging.error('cache_path ({:}) does not exist'.format(cache_path))
        return

    if not os.path.isdir(binary_path):
        logging.error('binary_path ({:}) does not exist'.format(binary_path))
        return
    
    if not os.path.isdir(ascii_path):
        logging.info('Making path at: {:}'.format(ascii_path))
        os.makedirs(ascii_path)


    #--------------------------------------------
    # Make sure cache files are lowercase
    files_list = os.listdir(cache_path)
    files_list_CAC = list(filter(lambda i: i.endswith(".CAC"), files_list))

    if len(files_list_CAC) > 0:
        logging.info('{:} .CAC files will be renamed'.format(len(files_list_CAC)))
        run_out = subprocess.run([cac2lower_file, os.path.join(cache_path, "*")])

        if run_out.returncode != 0:
            logging.error('Error running `{:} {:}`'.format(cac2lower_file, os.path.join(cache_path, "*")))
            return

        # Make sure that all .CAC files have corresponding .cac files before deleting
        delete_ok = True
        files_list_new = os.listdir(cache_path)

        for i in files_list_CAC:
            if i.lower() not in files_list_new:
                delete_ok = False

        if delete_ok: 
            run_out = subprocess.run(["find", cache_path, '-name', '*.CAC', '-delete'])
            if run_out.returncode != 0:
                logging.error('Error running `find {:} -name *.CAC -delete`'.format(cache_path))
                return

            logging.info("{:} uppercase .CAC files were deleted".format(len(files_list_CAC)))
        else:
            logging.warn("Not all '.CAC' files have a corresponding '.cac' file, and thus the .CAC files were not deleted")

    else:
        logging.info('There are no .CAC files to rename')


    #--------------------------------------------
    # Make dba files
    run_out = subprocess.run([processDbds_file, "-c", cache_path, binary_path, ascii_path], capture_output=True)    
    if run_out.returncode != 0:
        logging.error('Error running `{:}`'.format(processDbds_file))
        logging.error('Args: {:}'.format(run_out.args))
        logging.error('stderr: {:}'.format(run_out.stderr))
        return
    else:
        logging.info('Successfully completed run of `{:}`'.format(processDbds_file))




if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('binary_path', type=str,
                            help='Location of binary ([dest]bd) files')

    arg_parser.add_argument('ascii_path', type=str,
                            help='Path to write ascii (dba) files. If it does not exist, this path will be created')

    arg_parser.add_argument('cache_path', type=str,
                            help='Location of cache files')

    arg_parser.add_argument('processDbds_file', type=str,
                            help='Path to processDbds shell script')

    arg_parser.add_argument('cac2lower_file', type=str,
                            default = '/opt/slocum/bin2ascii/cac2lower.sh', 
                            help='Location of cache files')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
