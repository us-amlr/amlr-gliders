#!/bin/bash

# Installation, etc., for glider-proc VM
# $GROUP group must have been created first

# Explored options for checking for dpkg lock, but these are inconsistent
#   https://itsfoss.com/could-not-get-lock-error/
# Thus, restart the VM at least once before running this install script

# NOTE
# Installing miniconda requires users interaction; see notes below in script

# NOTE: 
# The bucket mounts are in the users $HOME dir to avoid permissions issues. 
# This is because, if created outisde of $HOME, functionally a user 
# needs to be owner these dirs to be able to use them.
# To allow for multiple potential users, each will create them in their $HOME

# NOTE: 
# The user must ssh into the SFMC from the command line once
# after recreating the VM, eg `ssh swoodman@sfmc.webbresearch.com` 
# and enter their password to accept the connection to sfmc.webbresearch.com


# # Mar 2023 to avoid public key issue
# curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -

GROUP=amlr-gliders #create group before running this script
sudo apt-get update


# Make directories, prep for hardened VM permissions
mkdir $HOME/tmp 
export TMPDIR=$HOME/tmp

sudo mkdir /var/sfmc
sudo chgrp -R $GROUP /var/sfmc
sudo chmod -R 770 /var/sfmc


# Install packages
export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
sudo apt-get update

sudo apt-get install -y gcsfuse rsync sshpass members


# Confirm group members:
echo "$GROUP GROUP MEMBERS:"
members $GROUP


# Clone GitHub repos to /opt, and manage permissions
sudo git clone https://github.com/kerfoot/slocum.git /opt/slocum
sudo chgrp -R $GROUP /opt/slocum 
sudo chmod -R 770 /opt/slocum

sudo git clone https://github.com/smwoodman/gdm.git /opt/gdm
sudo chgrp -R $GROUP /opt/gdm 
sudo chmod -R 770 /opt/gdm

sudo git clone https://github.com/us-amlr/amlr-gliders.git /opt/amlr-gliders
sudo chgrp -R $GROUP /opt/amlr-gliders
sudo chmod -R 770 /opt/amlr-gliders
sudo git -C /opt/amlr-gliders config --local core.fileMode false

# Create git pull script for development; must update permissions
# To think about if used by others: move this to amlr-gliders/resources
echo '#!/bin/bash

REPO_PATH=/opt/amlr-gliders
sudo git -C $REPO_PATH pull 
echo "Updating repo permissions"
sudo chgrp -R amlr-gliders $REPO_PATH
sudo chmod -R 770 $REPO_PATH
' > $HOME/gitpull-amlr-gliders.sh
sudo chmod 700 gitpull-amlr-gliders.sh


# Prep for mounting buckets
mkdir $HOME/amlr-gliders-deployments-dev
mkdir $HOME/amlr-imagery-raw-dev

# Make startup script - accessible to all users
sudo touch /opt/startup_glider_proc.sh
sudo chgrp $GROUP /opt/startup_glider_proc.sh
sudo chmod 770 /opt/startup_glider_proc.sh
echo '#!/bin/bash

BUCKET_PATH_DEPLOYMENTS=$HOME/amlr-gliders-deployments-dev
BUCKET_PATH_IMAGERY=$HOME/amlr-imagery-raw-dev

if [ ! -d "$HOME/tmp" ]
then
  mkdir $HOME/tmp
fi

if [ ! -d "$BUCKET_PATH_DEPLOYMENTS" ]
then
  mkdir $BUCKET_PATH_DEPLOYMENTS
fi

if [ ! -d "$BUCKET_PATH_IMAGERY" ]
then
  mkdir $BUCKET_PATH_IMAGERY
fi

export TMPDIR=$HOME/tmp
gcsfuse --implicit-dirs amlr-gliders-deployments-dev $BUCKET_PATH_DEPLOYMENTS/
gcsfuse --implicit-dirs amlr-imagery-raw-dev $BUCKET_PATH_IMAGERY/
' > /opt/startup_glider_proc.sh


# Install conda to /opt
sudo mkdir /opt/anaconda
sudo chgrp -R $GROUP /opt/anaconda
sudo chmod 770 -R /opt/anaconda

# # TODO: use -p flag to pass path (prefix) /opt/anaconda/miniconda3
# # TODO: maybe run with -b flag to run in batch mode (with no manual intervention)?
curl -O https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh
bash Miniconda3-py39_4.12.0-Linux-x86_64.sh
# Install path: /opt/anaconda/miniconda3
# Respond 'yes' to conda init question

# curl -O https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh
# bash Anaconda3-2021.11-Linux-x86_64.sh
# # Install path: /opt/anaconda/anaconda3
# # Respond 'yes' to conda init question


# Update permission things after install
sudo chgrp -R $GROUP /opt/anaconda
sudo chmod 770 -R /opt/anaconda

echo "GLIDER-PROC INSTALL COMPLETE"
echo "To finish setup, run the various conda setup commands"

# # To be able to use conda command. Can only run in interactive shell
# source $HOME/.bashrc 

# # Conda setup commands:
# conda deactivate && conda config --set auto_activate_base false && conda update -yn base -c defaults conda
# conda env create -f /opt/amlr-gliders/environment.yml && conda activate amlr-gliders &&pip install -e amlr-gliders
# sudo chgrp -R $GROUP /opt/anaconda && sudo chmod 770 -R /opt/anaconda
