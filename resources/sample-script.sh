#!/bin/bash

# This is a sample script for processing AMLR glider data in GCP
# Users should always update PROJECT, DEPLOYMENT, and MODE, 
#   as well as which scripts should be run

PROJECT=FREEBYRD
DEPLOYMENT=amlr06-20221205
MODE=delayed

# Deal with hardened VM permissions, and prep for mounted buckets
sudo mount -o remount,exec /tmp
PATH_DEPLOYMENTS=/tmp/amlr-gliders-deployments-dev
PATH_IMAGERY=/tmp/amlr-imagery-raw-dev
AMLR_GLIDERS=/opt/amlr-gliders

if [ ! -d "$PATH_DEPLOYMENTS" ]
then
  mkdir $PATH_DEPLOYMENTS
fi

if [ ! -d "$PATH_IMAGERY" ]
then
  mkdir $PATH_IMAGERY
fi

gcsfuse --implicit-dirs amlr-gliders-deployments-dev $PATH_DEPLOYMENTS/
gcsfuse --implicit-dirs amlr-imagery-raw-dev $PATH_IMAGERY/

source /opt/anaconda/miniconda3/etc/profile.d/conda.sh
conda activate amlr-gliders

# # TODO: if log file doesn't exist, make it?

# Scrape data from the SFMC, convert the binary files to DBA files, and create the trajectory NetCDF files
$AMLR_GLIDERS/scripts/amlr_scrape_sfmc.py $DEPLOYMENT $PROJECT
$AMLR_GLIDERS/scripts/amlr_binary_to_dba.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS 
$AMLR_GLIDERS/scripts/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --write_trajectory --write_acoustics --write_imagery --imagery_path=$PATH_IMAGERY

exit 0
