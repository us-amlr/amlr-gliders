#!/bin/bash

# Commands to run when you first ssh into the VM to mount the buckets, activate the amlr-gliders conda environment, and set general variables.
# conda env update --file /opt/amlr-gliders//environment.yml --prune
/opt/startup_glider_proc.sh
source /opt/anaconda/miniconda3/etc/profile.d/conda.sh
conda activate amlr-gliders
# python -m pip install -e git+https://github.com/us-amlr/amlr-gliders.git#egg=amlr_gliders

# TODO: if log file doesn't exist, make it

# Set path variables
PATH_DEPLOYMENTS=$HOME/amlr-gliders-deployments-dev
PATH_IMAGERY=$HOME/amlr-imagery-raw-dev
AMLR_GLIDERS=/opt/amlr-gliders

# Set deployemnt-specific variables
PROJECT=FREEBYRD
DEPLOYMENT=amlr06-20221205
MODE=delayed

# Scrape data from the SFMC, convert the binary files to DBA files, and create the trajectory NetCDF files
$AMLR_GLIDERS/scripts/amlr_scrape_sfmc.py $DEPLOYMENT $PROJECT
$AMLR_GLIDERS/scripts/amlr_binary_to_dba.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS
$AMLR_GLIDERS/scripts/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --write_trajectory --write_acoustics


# $AMLR_GLIDERS/scripts/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --write_trajectory

# $AMLR_GLIDERS/scripts/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --loadfromtmp --write_acoustics --write_imagery --imagery_path=$PATH_IMAGERY

# $AMLR_GLIDERS/scripts/amlr_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $PATH_DEPLOYMENTS --loglevel=debug

