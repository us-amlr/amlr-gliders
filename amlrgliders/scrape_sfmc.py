"""
Functions used when scraping and organizing AMLR glider data from the SFMC
"""

import os
import logging
from amlrgliders.utils import path_check

logger = logging.getLogger(__name__)


def line_prepender(filename, line):
    """
    Title: prepend-line-to-beginning-of-a-file
    https://stackoverflow.com/questions/5914627
    """
    
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)


def access_secret_version(project_id, secret_id, version_id = 'latest'):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    
    https://github.com/googleapis/python-secret-manager/blob/main/samples/snippets/access_secret_version.py
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


def rt_file_management(sfmc_ext_all, ext, ext_regex, ext_name, local_path, subdir_path, bucket_path):
    """
    Copy real-time files from the local sfmc folder (local_path)
    to their subdirectory (subdir_path), 
    and then rsync to their place in the bucket (bucket_path)

    # TODO: only use one of ext and ext_regex?
    """
    if (any([i in sfmc_ext_all for i in ext])):
    # if (ext in sfmc_ext_all):
        path_check(local_path)
        path_check(subdir_path)
        path_check(bucket_path)

        logging.info(f'Moving {ext_name} files to their local subdirectory')
        ext_regex_path = os.path.join(local_path, ext_regex)
        logging.debug(f'Regex extension path: {ext_regex_path}')
        logging.debug(f'Local subdirectory: {subdir_path}')
        retcode_tmp = call(f'rsync {ext_regex_path} {subdir_path}', 
            shell = True)

        logging.info(f'rsync-ing {ext_name} subdirectory with bucket directory')
        logging.debug(f'Bucket directory: {bucket_path}')
        retcode = run(['gsutil', '-m', 'rsync', '-r', ext_regex_path, bucket_path], 
            capture_output = True)
        if retcode.returncode != 0:
            logging.error(f'Error copying {ext_name} files to bucket')
            logging.error(f'Args: {retcode.args}')
            logging.error(f'stderr: {retcode.stderr}')
            return
        else:
            logging.info(f'Successfully copied {ext_name} files to {bucket_path}')
            logging.debug(f'Args: {retcode.args}')
            logging.debug(f'stderr: {retcode.stdout}')
    else: 
        logging.info(f'No {ext_name} files to copy')
