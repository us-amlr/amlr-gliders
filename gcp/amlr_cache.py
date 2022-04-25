import os
import logging

logging.basicConfig(level=logging.INFO)


# Prep
home_path = "/home/sam_woodman_noaa_gov"
bucket_cache = "amlr-gliders-deployments-dev/cache"

folder_cache = os.path.join(home_path, bucket_cache)
files_list = os.listdir(folder_cache)
files_list_CAC = list(filter(lambda i: i.endswith(".CAC"), files_list))
logging.info("{:} .CAC files will be renamed".format(len(files_list_CAC)))

# Run shell script
os.system("/opt/slocum/bin2ascii/cac2lower.sh " + os.path.join(folder_cache, "*"))

# Make sure that all .CAC files have corresponding .cac files before deleting
delete_ok = True
files_list_new = os.listdir(folder_cache)

for i in files_list_CAC:
    if i.lower() not in files_list_new:
        delete_ok = False

if delete_ok: 
    os.system("find " + folder_cache + " -name '*.CAC' -delete")
    logging.info("{:} uppercase .CAC files were deleted".format(len(files_list_CAC)))
else:
    logging.warn("Not all '.CAC' files have a corresponding '.cac' file, and thus the .CAC files were not deleted")
