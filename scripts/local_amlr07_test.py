# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 11:41:54 2022

@author: sam.woodman
"""

import os
import sys

# import multiprocessing as mp
# mp.cpu_count()

for d in sys.path: print(d)

smw_gliderdir = 'C:/SMW/Gliders_Moorings/Gliders'

sys.path.append(os.path.join(smw_gliderdir, 'gdm'))
sys.path.append(os.path.join(smw_gliderdir, 'amlr-gliders', 'amlr-gliders'))
from gdm import GliderDataModel
from amlr import amlr_gdm, amlr_acoustics, amlr_imagery, amlr_year_path


deployment = 'amlr07-20221025'
project = 'SANDIEGO'
mode = 'delayed'
deployments_path = os.path.join(smw_gliderdir, 'Glider-Data-gcp')

# gdm_path = args.gdm_path
numcores = 7

loadfromtmp = False
write_trajectory = False
write_ngdac = False
write_acoustics = False
write_imagery = False
imagery_path = ''


deployment_split = deployment.split('-')
year = amlr_year_path(project, deployment_split)
deployment_curr_path = os.path.join(deployments_path, project, year, deployment)
glider_path = os.path.join(deployment_curr_path, 'glider')

gdm = GliderDataModel(os.path.join(glider_path, 'config', 'gdm'))
# gdm = amlr_gdm(deployment, project, mode, glider_path, numcores, loadfromtmp)


### Test GliderTools
