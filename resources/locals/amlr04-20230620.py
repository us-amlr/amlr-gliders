# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 12:39:41 2023

By hand processing of data from amlr07-20221204 
Glider fried and lost data, so let's see what's in here.?

@author: sam.woodman
"""

import os
# import sys
# import gdm
import xarray as xr
# from gdm import GliderDataModel
# from gdm.gliders.slocum import load_slocum_dba
from amlrgliders.glider import amlr_gdm

# import multiprocessing as mp
# mp.cpu_count()

# for d in sys.path: print(d)

smw_gliderdir = 'C:/SMW/Gliders_Moorings/Gliders'

# sys.path.append(os.path.join(smw_gliderdir, 'gdm'))
# sys.path.append(os.path.join(smw_gliderdir, 'amlr-gliders', 'amlr-gliders'))
# from gdm import GliderDataModel
# from amlr import amlr_gdm, amlr_acoustics, amlr_imagery, amlr_year_path


deployment = 'amlr03-20230620'
project = 'SANDIEGO'
mode = 'delayed'
deployments_path = os.path.join(smw_gliderdir, 'Glider-Data')

# gdm_path = args.gdm_path
numcores = 7

loadfromtmp = False
write_trajectory = False
write_ngdac = False
write_acoustics = False
write_imagery = False
imagery_path = ''


deployment_split = deployment.split('-')
year = '2023'
deployment_curr_path = os.path.join(deployments_path, project, year, deployment)
glider_path = os.path.join(deployment_curr_path, 'glider')

# gdm = GliderDataModel(os.path.join(glider_path, 'config', 'gdm'))
# # gdm = amlr_gdm(deployment, project, mode, glider_path, numcores, loadfromtmp)
# dba_path = os.path.join(glider_path, 'data', 'in', 'ascii', 'rt')
# os.listdir(dba_path)
# dba, pro_meta = load_slocum_dba(os.path.join(dba_path, 'amlr07_2022_298_2_0_sbd.dat'))

# gdm.data = dba
# gdm.profiles = pro_meta

gdm = amlr_gdm(
    deployment, project, mode, glider_path, numcores, 
    loadfromtmp = True, clobbertmp = False
)
prof = gdm.profiles

# import xarray as xr
# ds = xr.open_dataset(os.path.join(
#     glider_path, 'data', 'out', 'nc', 'trajectory', 
#     f"{deployment}-{mode}-trajectory.nc"
# ))


# Do a little subsetting
gdm_data_vars_list = [
    'time', 'latitude', 'longitude', 
    'depth', 'm_depth', 'm_heading', 'm_pitch', 'm_roll', 
    "ilatitude", "ilongitude", 
    'idepth', 'impitch', 'imroll', 
    'sci_flbbcd_cdom_units', 'sci_water_cond', 'density', 
    'sci_water_pressure', 'salinity', 'sci_water_temp', 
    
    'sci_flbbcd_bb_units', 'sci_flbbcd_therm', 
    'sci_flbbcd_chlor_units', 'sci_flbbcd_cdom_units', 
    'sci_oxy4_oxygen', 'sci_oxy4_saturation', 'sci_oxy4_temp', 
    # 'sci_ctd41cp_timestamp', 
    'm_final_water_vx', 'm_final_water_vy', 
    'c_wpt_lat', 'c_wpt_lon'
    
    # 'cdom', 'conductivity', 'density', 'pressure', 
    # 'salinity', 'temperature', 'beta700', 'chlorophyll_a', 
    # 'oxy4_oxygen', 'oxy4_saturation', 
    # 'oxy4_temp', 'sci_flbbcd_therm', 'ctd41cp_timestamp', 
    # 'm_final_water_vx', 'm_final_water_vy', 'c_wpt_lat', 'c_wpt_lon'
]
# gdm.data.sci_flbbcd_therm

subset = sorted(set(gdm_data_vars_list).intersection(list(gdm.data.columns)), 
                    key = gdm_data_vars_list.index)
gdm.data = gdm.data[subset]

# gdm.data.latitude = gdm.data.ilatitude
# gdm.data.longitude = gdm.data.ilongitude

# prof = prof[prof.total_seconds >= 300]
# gdm.profiles = prof

# row = self._profiles_meta.loc[profile_time]
# pro_df = self._df.loc[row.start_time:row.end_time]
# pro_ds0 = gdm.slice_profile_dataset(gdm.profiles.index[0])

# pro_ds_list = []
# for n_prof in range((len(prof.index)-1)):
#     # n_prof=105
#     print(f"profile index {n_prof}, direction {row['direction']}")
#     row = prof.loc[prof.index[n_prof]]
#     ds = gdm.data.loc[row.start_time:row.end_time]
#     pro_ds = gdm.slice_profile_dataset(prof.index[n_prof])
#     pro_ds_list.append(pro_ds)
#     print("done")

nc_ngdac_path = os.path.join(glider_path, 'data', 'out', 'nc', 'ngdac', mode)
for profile_time, row, pro_ds in gdm.iter_profiles():
    # print(f"profile time: {profile_time}")
    pro_ds['profile_direction'] = row.direction
    nc_path = os.path.join(nc_ngdac_path, f"{deployment}_{profile_time.strftime('%Y%m%dT%H%M%S')}_{mode}.nc")
    # print('Writing {:}'.format(nc_path))
    # break
    pro_ds.to_netcdf(nc_path)


import xarray as xr
ds = xr.open_mfdataset(
    os.path.join(nc_ngdac_path, '*.nc'), 
    combine_attrs="drop_conflicts"
)
