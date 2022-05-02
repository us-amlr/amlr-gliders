#!/usr/bin/env python

import os
import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO)


def cac2lower(
    cache_path, 
    cac2lower_file = '/opt/slocum/bin2ascii/cac2lower.sh'
):
    """
    Wrapper around cac2lower.sh; makes cac files lowercase
    Requires Linux system with kerfoot/slocum cloned to /opt

    PARAMETERS:
    cache_path: string; path to folder with cache files
    cac2lower_file: string; path to cac2lower.sh file 

    Returns none
    """

    if not os.path.isfile(cac2lower_file):
        logging.error('The provided cac2lower_file does not exist: {:}'.format(cac2lower_file))
        return

    if not os.path.isdir(cache_path):
        logging.error('The provided cache_path does not exist: {:}'.format(cache_path))
        return

    # Get CAC files
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



def main(processDbds_file, cache_path, binary_path, ascii_path):
    """
    Wrapper around cac2lower.sh; makes cac files lowercase
    Requires Linux system with kerfoot/slocum cloned to /opt

    PARAMETERS:
    processDbds_file: string, path to file with processDbds shell script
    cache_path:     string, path to cache folder
    binary_path:    string, path to binary folder with [dest]bd files
    ascii_path:     string, path where ascii files should be written

    Returns none
    """

    ### Checks
    if not os.path.isfile(processDbds_file):
        logging.error('processDbds_file ({:}) does not exist'.format(processDbds_file))
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

    ### Make sure cache files are lowercase
    logging.info('Running cac2lower to ensure cache files are lowercase')
    cac2lower(cache_path)

    ### Create and run command line command
    # sys_command = processDbds_file + " -c " + cache_path + " " + \
    #     binary_path + " " + ascii_path
    # logging.info('Running command: {:}'.format(sys_command))

    # os.system(sys_command)
    run_out = subprocess.run([processDbds_file, "-c", cache_path, binary_path, ascii_path], capture_output=True)    
    if run_out.returncode != 0:
        logging.error('Error running `{:}`'.format(processDbds_file))
        logging.error('Args: {:}'.format(run_out.args))
        logging.error('stderr: {:}'.format(run_out.stderr))
        return
    else:
        logging.info('Successfully completed run of {:}'.format(processDbds_file))



if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]))
