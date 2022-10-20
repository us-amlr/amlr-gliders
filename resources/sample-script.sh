#!/bin/bash

# Sample script for scraping and processing real-time data from amlr03-20220513

$HOME/startup_glider_proc.sh
conda activate amlr-gliders

AMLR_GLIDERS=$HOME/amlr-gliders
PROJECT=SANDIEGO
DEPLOYMENT=amlr08-20220513
MODE=delayed
BUCKET_PATH=$HOME/amlr-gliders-deployments-dev
BUCKET_PATH_IMAGERY=$HOME/amlr-imagery-raw-dev

$AMLR_GLIDERS/scripts/amlr_scrape_sfmc.py $DEPLOYMENT $PROJECT
$AMLR_GLIDERS/scripts/amlr_binary_to_dba.py $DEPLOYMENT $PROJECT $MODE $BUCKET_PATH $HOME/amlr-gliders/resources/processDbds_usamlr.sh
$AMLR_GLIDERS/scripts/amlr_process_glider.py $DEPLOYMENT $PROJECT $MODE $BUCKET_PATH --numcores=16 --write_trajectory

echo "All done"
