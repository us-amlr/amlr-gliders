# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 12:39:41 2023

By hand processing of data from amlr06-20221205

Playing with how to not run out of memory in GCP VM

@author: sam.woodman
"""

import os
# import sys
import gdm
from gdm import GliderDataModel
from gdm.gliders.slocum import load_slocum_dba

import multiprocessing as mp
import pandas as pd
# import dask.dataframe as dd


# import multiprocessing as mp
# mp.cpu_count()

# for d in sys.path: print(d)

if __name__ == '__main__':
    smw_gliderdir = 'C:/SMW/Gliders_Moorings/Gliders'

    deployment = 'amlr06-20221205'
    project = 'FREEBYRD'
    mode = 'delayed'
    deployments_path = smw_gliderdir #os.path.join(smw_gliderdir, 'Glider-Data-gcp')
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

    # # Works, for reference
    # x1 = pd.concat(dba_zip)
    # x2 = pd.DataFrame()
    # for idx, i in enumerate(dba_zip):
    #     print(f'df {idx}')
    #     x2 = pd.concat([x2, i])
    # print(x2.equals(x1))

    ###
    dba_list = list(dba_zip)
    chunksize = 6
    total_len = len(dba_files_list)
    tmp_path = os.path.join('C:/Users', 'sam.woodman', 'Downloads', 'tmp')
    pqt_file_name = []
    z1 = pd.concat(dba_zip)
    z1.sort_index(inplace = True)
    
    for idx_start in range(0, total_len, chunksize):
        idx_end = min(idx_start+chunksize, total_len)
        print(idx_start, idx_end)
        tmp_df = pd.concat(dba_list[idx_start:idx_end])
        tmp_file_name = os.path.join(tmp_path, f'{deployment_mode}-tmp-{idx_start}-{idx_end}.parquet')
        pqt_file_name.append(tmp_file_name)
        tmp_df.to_parquet(tmp_file_name, version="2.6", index = True)


    pl_df = pl.read_parquet(
        os.path.join(tmp_path, '*.parquet')
    )
    z2 = pl_df.to_pandas().set_index('time')
    z2.sort_index(inplace=True)
    z1.equals(z2)
    
    t2 = pl.from_pandas(pl_df.to_pandas())
    
    gdm.data = pl_df
    ###
    

            
        


    tmp_pqt_path = os.path.join('C:/Users', 'sam.woodman', 'Downloads', 'tmp')
    tmp_pqt_file = os.path.join(tmp_pqt_path, 'dba_pqt_all.parquet')

        # with tmp_pqt_file 
    # for i in dba_zip_list[0]:
    #     print(len(i.index))

    pro_meta = pd.concat(dba_zip_list[1])

    gdm.data = dba 
    gdm.profiles = pro_meta

    print(gdm)
