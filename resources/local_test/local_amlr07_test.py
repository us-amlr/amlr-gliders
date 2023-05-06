# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 11:41:54 2022

@author: sam.woodman
"""

import os
# import sys
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


deployment = 'amlr07-20221025'
project = 'SANDIEGO'
mode = 'rt'
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
year = '2022'
deployment_curr_path = os.path.join(deployments_path, project, year, deployment)
glider_path = os.path.join(deployment_curr_path, 'glider')

gdm = GliderDataModel(os.path.join(glider_path, 'config', 'gdm'))
# gdm = amlr_gdm(deployment, project, mode, glider_path, numcores, loadfromtmp)
dba_path = os.path.join(glider_path, 'data', 'in', 'ascii', 'rt')
os.listdir(dba_path)
dba, pro_meta = load_slocum_dba(os.path.join(dba_path, 'amlr07_2022_298_2_0_sbd.dat'))

gdm.data = dba
gdm.profiles = pro_meta

# explore interpolation
import xarray as xr
import pandas as pd

rt_ds = xr.open_dataset(os.path.join(glider_path, 'data', 'out', 'nc', 'trajectory', 'amlr07-20221025-rt-trajectory-full.nc'))
rt_ds[['depth', 'm_depth', 'idepth']].to_pandas().describe()


gdm.data.depth.describe()
gdm.data['idepth'] = gdm.data.depth.interpolate(method='time', limit_direction='forward', limit_area='inside')
gdm.data[['depth', 'idepth']].describe()

import logging
logger = logging.getLogger(__name__)

def amlr_interpolate(df, var_src, var_dst):
    if var_src in df:
        logger.info(f'Creating interpolated data column ({var_dst}) from {var_src}')
        df[var_dst] = df[var_src].interpolate(method='time', limit_direction='forward', limit_area='inside')
    else:
        logger.info(f'No {var_src} variable, and thus {var_dst} will not be created')

    return df
    
var_src = 'm_roll'
var_dst = 'imroll'
gdm.data = amlr_interpolate(gdm.data, "m_roll", "imroll")

'm_orroll' in gdm.data.columns

### Test GliderTools
