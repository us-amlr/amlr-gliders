# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 12:39:41 2023

By hand processing of data from amlr06-20221205

Priority one - determine cause of and fix for warning message:
---
FutureWarning: The behavior of 'to_datetime' with 'unit' when 
parsing strings is deprecated. 
In a future version, strings will be parsed as datetime strings, 
matching the behavior without a 'unit'. 
To retain the old behavior, explicitly cast ints or floats to numeric type 
before calling to_datetime.
---

@author: sam.woodman
"""

import os
# import sys
import gdm
from gdm import GliderDataModel
from gdm.gliders.slocum import load_slocum_dba

# import multiprocessing as mp
# mp.cpu_count()

# for d in sys.path: print(d)

smw_gliderdir = 'C:/SMW/Gliders_Moorings/Gliders'

# sys.path.append(os.path.join(smw_gliderdir, 'gdm'))
# sys.path.append(os.path.join(smw_gliderdir, 'amlr-gliders', 'amlr-gliders'))
# from gdm import GliderDataModel
# from amlr import amlr_gdm, amlr_acoustics, amlr_imagery, amlr_year_path


deployment = 'amlr06-20221205'
project = 'FREEBYRD'
mode = 'delayed'
deployments_path = smw_gliderdir #os.path.join(smw_gliderdir, 'Glider-Data-gcp')
# gdm_path = args.gdm_path
numcores = 7

loadfromtmp = False
write_trajectory = False
write_ngdac = False
write_acoustics = False
write_imagery = False
imagery_path = ''


deployment_split = deployment.split('-')
year = '2022'
# deployment_curr_path = os.path.join(deployments_path, project, year, deployment)
deployment_curr_path = os.path.join(deployments_path, deployment)
glider_path = os.path.join(deployment_curr_path, 'glider')

gdm = GliderDataModel(os.path.join(glider_path, 'config', 'gdm'))
# gdm = amlr_gdm(deployment, project, mode, glider_path, numcores, loadfromtmp)
dba_path = os.path.join(glider_path, 'data', 'in', 'ascii', mode)
os.listdir(dba_path)
os.listdir(dba_path)[10]
dba, pro_meta = load_slocum_dba(os.path.join(dba_path, os.listdir(dba_path)[10]))

gdm.data = dba
gdm.profiles = pro_meta

print(gdm)
