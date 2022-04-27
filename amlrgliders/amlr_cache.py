#!/usr/bin/env python

def amlr_cache(
    cache_path = "/home/sam_woodman_noaa_gov/amlr-gliders-deployments-dev/cache"
):
    """
    Wrapper around cac2lower.sh; makes cac files lowercase
    Requires Linux system with kerfoot/slocum cloned to /opt

    PARAMETERS:
    cache_path: string, path to folder with cache files. 
        Default corresponds to path on GCP VM.

    Returns none
    """

    import os
    import logging

    logging.basicConfig(level=logging.INFO)

    cac2lower_file = "/opt/slocum/bin2ascii/cac2lower.sh"
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
        os.system(cac2lower_file + " " + os.path.join(cache_path, "*"))

        # Make sure that all .CAC files have corresponding .cac files before deleting
        delete_ok = True
        files_list_new = os.listdir(cache_path)

        for i in files_list_CAC:
            if i.lower() not in files_list_new:
                delete_ok = False

        if delete_ok: 
            os.system("find " + cache_path + " -name '*.CAC' -delete")
            logging.info("{:} uppercase .CAC files were deleted".format(len(files_list_CAC)))
        else:
            logging.warn("Not all '.CAC' files have a corresponding '.cac' file, and thus the .CAC files were not deleted")

    else:
        logging.info('There are no .CAC files to rename')
