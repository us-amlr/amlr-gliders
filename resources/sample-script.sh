#!/bin/bash

# Sample script for scraping and processing real-time data from deployment amlr07-20221025

/opt/startup_glider_proc.sh
conda activate amlr-gliders

BUCKET_PATH=$HOME/amlr-gliders-deployments-dev
BUCKET_PATH_IMAGERY=$HOME/amlr-imagery-raw-dev
AMLR_GLIDERS=/opt/amlr-gliders

PROJECT=SANDIEGO
DEPLOYMENT=amlr07-20221025
MODE=delayed

$AMLR_GLIDERS/scripts/amlr_scrape_sfmc.py $DEPLOYMENT $PROJECT
$AMLR_GLIDERS/scripts/amlr_binary_to_dba.py $DEPLOYMENT $PROJECT $MODE $BUCKET_PATH
$AMLR_GLIDERS/scripts/amlr_process_dba_to_nc.py $DEPLOYMENT $PROJECT $MODE $BUCKET_PATH --write_trajectory --write_acoustics

echo "All done"
