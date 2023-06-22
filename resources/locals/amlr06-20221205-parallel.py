# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 12:39:41 2023

By hand processing of data from amlr06-20221205

Playing with how to not run out of memory in GCP VM

@author: sam.woodman
"""

import os
import glob
# import sys

import gdm
from gdm import GliderDataModel
from gdm.gliders.slocum import load_slocum_dba

import multiprocessing as mp
import pandas as pd
from amlrgliders.glider import amlr_interpolate, solocam_filename_dt
# import dask.dataframe as dd


# import multiprocessing as mp
# mp.cpu_count()

# for d in sys.path: print(d)

if __name__ == '__main__':
    smw_gliderdir = 'C:/SMW/Gliders_Moorings/Gliders'

    deployment = 'amlr06-20221205'
    project = 'FREEBYRD'
    mode = 'delayed'
    deployments_path = os.path.join(
        smw_gliderdir, 'Glider-Data', 'FREEBYRD', '2022-23'
    )
    # gdm_path = args.gdm_path
    numcores = 5

    loadfromtmp = False
    write_trajectory = False
    write_ngdac = False
    write_acoustics = False
    write_imagery = False
    imagery_path = ''


    deployment_split = deployment.split('-')
    deployment_mode = f'{deployment}-{mode}'
    year = '2022'
    # deployment_curr_path = os.path.join(deployments_path, project, year, deployment)
    deployment_curr_path = os.path.join(deployments_path, deployment)
    glider_path = os.path.join(deployment_curr_path, 'glider')
    ascii_path = os.path.join(glider_path, 'data', 'in', 'ascii', mode)

    gdm = GliderDataModel(os.path.join(glider_path, 'config', 'gdm'))

    dba_files_list_all = list(map(lambda x: os.path.join(ascii_path, x), os.listdir(ascii_path)))
    # dba_files = pd.DataFrame(dba_files_list, columns = ['dba_file'])

    dba_files_list = dba_files_list_all[:21]

    # for index, row in dba_files[:3].iterrows():
    #     # dba_file = os.path.join(row['path'], row['file'])
    #     dba, pro_meta = load_slocum_dba(row['dba_file'])
        
    #     gdm.data = pd.concat([gdm.data, dba])
    #     gdm.profiles = pd.concat([gdm.profiles, pro_meta])


    print(f'Reading ascii data into gdm object using {numcores} core(s)')
    pool = mp.Pool(numcores)
    load_slocum_dba_list = pool.map(load_slocum_dba, dba_files_list)
    pool.close()   
    # with mp.Pool(numcores) as pool:
    #     load_slocum_dba_list = pool.map(load_slocum_dba, dba_files_list)

    load_slocum_dba_zip = zip(*load_slocum_dba_list)
    dba_zip_list = list(load_slocum_dba_zip)

    dba_zip, pro_meta_zip = zip(*load_slocum_dba_list)

    dba_df = pd.concat(dba_zip)
    pro_meta = pd.concat(pro_meta_zip)

    # dba_dup = dba_df.index.duplicated(keep='last')

    # tmp_pqt_path = os.path.join('C:/Users', 'sam.woodman', 'Downloads', 'tmp')
    # tmp_pqt_file = os.path.join(tmp_pqt_path, 'dba_pqt_all.parquet')

    # with tmp_pqt_file 
    # for i in dba_zip_list[0]:
    #     print(len(i.index))


    gdm.data = dba_df.sort_index()
    gdm.profiles = pro_meta.sort_index()
    
    gdm.data['idepth']  = amlr_interpolate(gdm.data['depth'])
    gdm.data['imdepth'] = amlr_interpolate(gdm.data['m_depth'])
    gdm.data.loc[:, 'impitch'] = amlr_interpolate(gdm.data['m_pitch'])
    gdm.data.loc[:, 'imroll']  = amlr_interpolate(gdm.data['m_roll'])

    print(gdm)
    
    ### Imagery
    # amlr_imagery(
    # gdm, deployment, glider_path, 
    imagery_path = os.path.join(smw_gliderdir, 'Glider-Imagery', 'gliders', '2022', 
                    'amlr06-20221205', 'glidercam', 'images')
    ext = 'jpg'
    # )
    
    lat_column = 'ilatitude'
    lon_column = 'ilongitude'
    depth_column = 'idepth'
    pitch_column = 'impitch'
    roll_column = 'imroll'
    out_path = os.path.join(glider_path, 'data', 'out', 'cameras')
    
    imagery_filepaths = glob.glob(f'{imagery_path}/**/*.{ext}', recursive=True)
    imagery_files = [os.path.basename(x) for x in imagery_filepaths]
    imagery_files.sort()
    
    imagery_vars_list = [lat_column, lon_column, depth_column, pitch_column, roll_column]
    imagery_vars_set = set(imagery_vars_list)
    gdm.data = gdm.data[imagery_vars_list]
    ds = gdm.data.sort_index().to_xarray()
    
    len(set([len(i) for i in imagery_files]))
    space_index = str.index(imagery_files[0], ' ')
    yr_index = space_index + 1   
    imagery_file_dts = [solocam_filename_dt(i, yr_index) for i in imagery_files]

    imagery_dict = {'img_file': imagery_files, 'img_dt': imagery_file_dts}
    imagery_df = pd.DataFrame(data = imagery_dict).sort_values('img_dt')

    ds_slice = ds.sel(time=imagery_df.img_dt.values, method = 'nearest')

    x = (imagery_df.img_dt - imagery_df.glider_dt).astype('timedelta64[s]')
    x = x.dt.total_seconds()
    
    y = (imagery_df.img_dt - imagery_df.glider_dt).dt.total_seconds()

    imagery_df['glider_dt'] = ds_slice.time.values
    imagery_df['diff_dt_seconds'] = diff_dts.dt.total_seconds()
    imagery_df['latitude'] = ds_slice[lat_column].values
    imagery_df['longitude'] = ds_slice[lon_column].values
    imagery_df['depth'] = ds_slice[depth_column].values
    imagery_df['pitch'] = ds_slice[pitch_column].values
    imagery_df['roll'] = ds_slice[roll_column].values
