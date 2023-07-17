#!/bin/bash

# This is a sample script for processing AMLR glider data in GCP
# Users should always update PROJECT, DEPLOYMENT, and MODE, 
#   as well as which scripts should be run

PROJECT=FREEBYRD
DEPLOYMENT=amlr06-20221205
MODE=delayed
YR_DIR='2022-23'

# Prep for and mount buckets
BUCKET_DIR=/mnt/gcs
PATH_DEPLOYMENTS=$BUCKET_DIR/deployments
PATH_IMAGERY=$BUCKET_DIR/imagery

for i in $PATH_DEPLOYMENTS $PATH_IMAGERY $HOME/tmp
do
    if [ ! -d "$i" ]
    then
      mkdir -p $i
    fi
done

export TMPDIR=$HOME/tmp #TODO: move this to somewhere in Python
gcsfuse --implicit-dirs amlr-gliders-deployments-dev $PATH_DEPLOYMENTS/
gcsfuse --implicit-dirs amlr-imagery-raw-dev $PATH_IMAGERY/

# Activate conda environment and run Python scripts
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate amlr-gliders
AGSCRIPTS=/opt/amlr-gliders/scripts
LOGDIR=/tmp/logs
mkdir -p $LOGDIR
LOGFILE=$LOGDIR/$DEPLOYMENT-$MODE.log

#Scrape data from the SFMC
$AGSCRIPTS/amlr_scrape_sfmc.py $DEPLOYMENT $PROJECT --logfile=$LOGFILE
#Convert the binary files to DBA files
$AGSCRIPTS/amlr_binary_to_dba.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS \
  --logfile=$LOGFILE
#Create desired products, such as the trajectory NetCDF files
$AGSCRIPTS/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS \
  --clobbertmp --write_trajectory --logfile=$LOGFILE
#--write_acoustics --write_imagery --imagery_path=$PATH_IMAGERY --loglevel=DEBUG

# Cleanup: unmount buckets, copy log files to bucket
fusermount -u $PATH_DEPLOYMENTS
fusermount -u $PATH_IMAGERY
gsutil cp $LOGDIR/* \
  gs://amlr-gliders-deployments-dev/$PROJECT/$YR_DIR/$DEPLOYMENT/scripts/logs/

exit 0
