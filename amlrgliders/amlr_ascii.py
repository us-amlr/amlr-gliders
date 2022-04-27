#!/usr/bin/env python

def amlr_process_binary(processDbds_file, cache_path, binary_path, ascii_path):
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

    import os
    import logging
    from . import amlr_cache

    logging.basicConfig(level=logging.INFO)

    ### Checks
    if not os.path.isfile(processDbds_file):
        logging.error('{:} does not exist'.format(processDbds_file))
        return

    if not os.path.isdir(cache_path):
        logging.error('{:} does not exist'.format(cache_path))
        return

    if not os.path.isdir(binary_path):
        logging.error('{:} does not exist'.format(binary_path))
        return
    
    if not os.path.isdir(ascii_path):
        logging.info('Making path at: {:}'.format(ascii_path))
        os.mkdir(ascii_path)
        return

    ### Make sure cache files are lowercase
    logging.info('Running amlr_cache to ensure cache files are lowercase')
    amlr_cache.amlr_cache(cache_path)

    ### Create and run command line command
    sys_command = processDbds_file + " -c " + cache_path + " " + \
        binary_path + " " + ascii_path
    logging.info('Running command: {:}'.format(sys_command))

    os.system(sys_command)
