#!/bin/bash

# Sam's test version of sample_script.sh

# PROJECT=FREEBYRD
# DEPLOYMENT=amlr06-20221205
# MODE=delayed
PROJECT=SANDIEGO
DEPLOYMENT=amlr03-20230620
MODE=rt

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

# # conda env update --file /opt/amlr-gliders//environment.yml --prune
source /opt/anaconda/miniconda3/etc/profile.d/conda.sh
conda activate amlr-gliders
# # python -m pip install -e git+https://github.com/us-amlr/amlr-gliders.git#egg=amlr_gliders

# # TODO: if log file doesn't exist, make it?

# Scrape data from the SFMC, convert the binary files to DBA files, and create the trajectory NetCDF files
$AMLR_GLIDERS/scripts/amlr_scrape_sfmc.py $DEPLOYMENT $PROJECT --logfile=$LOGFILE
$AMLR_GLIDERS/scripts/amlr_binary_to_dba.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --logfile=$LOGFILE
$AMLR_GLIDERS/scripts/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --write_trajectory --write_acoustics --logfile="/tmp/${DEPLOYMENT}_dba_to_nc.log" 

# $AMLR_GLIDERS/scripts/amlr_scrape_sfmc.py $DEPLOYMENT $PROJECT --logfile="/tmp/${DEPLOYMENT}_amlr_scrape_sfmc.log"
# $AMLR_GLIDERS/scripts/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --write_trajectory
# $AMLR_GLIDERS/scripts/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --loadfromtmp --write_acoustics --write_imagery --imagery_path=$PATH_IMAGERY
# $AMLR_GLIDERS/scripts/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --loglevel=debug

