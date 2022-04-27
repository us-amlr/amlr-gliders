#!/usr/bin/env python

import os
import sys
import logging

import pdb

def main(
    project, 
    deployment, 
    mode, 
    deployments_path, 
    gdm_path, 
    num_cores = 1, 
    load_from_tmp = False, 
    remove_19700101 = True, 
    save_trajectory = False, 
    save_ngdac = False
):
    
    """
    Process raw AMLR glider data and write data to parquet and nc files.

    This script depends requires the directory structure specified in the 
    AMLR glider data management readme:
    https://docs.google.com/document/d/1X5DB4rQRBhBqnFdAAY_5Eyh_yPjG3UZ43Ga7ZGWcSsg

    Returns a gdm object. Note the gdm object in the tmp file has not 
    removed 1970-01-01 timestamps or made column names lowercase. 
    
    PARAMETERS:
    project: string; name of project. 
        Should be one of FREEBYRD, REFOCUS, or SANDIEGO
    deployment: string; name of deployment, eg 'amlr03-20220308'. 
        The year is expected to be in characters 7:11
    mode: string; data mode must be either 'rt' or 'delayed'
    deployments_path: string, path to glider deployments folder
    gdm_path: path to gdm module

    OPTIONAL PARAMETERS:
    num_cores: numeric: indicates the number of cores to use. 
        If greater than one, parallel processing via mp.Pool.map will be used
        for load_slocum_dbas and (todo) writing individual (profile) nc files
        This argument must be between 1 and mp.cpu_count()
    load_from_tmp: boolean; indicates gdm object should be loaded from 
        parquet files in glider/data/tmp directory 
    remove_19700101: boolean; indicates if data with the timestamp 1970-01-01
        should be removed, before writing to pkl file. 
        Will be ignored if load_from_tmp = True
        For when there is a 'Not enough timestamps for yo interpolation' warning
    save_trajectory: boolean; indicates if trajectory should be saved to a nc file
    """

    import pandas as pd
    import multiprocessing as mp

    logging.basicConfig(level=logging.INFO)

    logging.info('gdm path: {:}'.format(gdm_path))
    pdb.set_trace()
    sys.path.append(gdm_path)


    from gdm import GliderDataModel
    from gdm.gliders.slocum import load_slocum_dba #, get_dbas

    

    
    ### Argument checks
    if not (project in ['FREEBYRD', 'REFOCUS', 'SANDIEGO']):
        logging.error("project must be one of 'FREEBYRD', 'REFOCUS', or 'SANDIEGO'")
        return 
    
    if not (mode in ['delayed', 'rt']):
        logging.error("mode must be either 'delayed' or 'rt'")
        return
    
    if not (1 <= num_cores and num_cores <= mp.cpu_count()):
        logging.error('num_cores must be between 1 and {:}'.format(mp.cpu_count()))
        return 
    
    if not all(x in os.listdir(deployments_path) for x in ['FREEBYRD', 'REFOCUS', 'SANDIEGO', 'cache']):
        str1 = 'The expected folders (FREEBYRD, REFOCUS, SANDIEGO, cache) were not found in the provided directory'
        str2 = 'Did you provide the right path to deployments_path?'
        logging.error((str1 + ' ({:}). ' + str2).format(deployments_path))
        return 
        
    if mode == 'delayed':
        binary_folder = 'debd'
    else: 
        binary_folder = 'stbd'
        

    ### Set path/file variables, and create file paths if necessary
    year = deployment[7:11]
    logging.info('Year extracted from deployment name: {:}'.format(year))
    deployment_mode = deployment + '-' + mode
    deployment_path = os.path.join(deployments_path, project, year, deployment, 'glider')
    logging.info('Deployment path: {:}'.format(deployment_path))

    ascii_path  = os.path.join(deployment_path, 'data', 'in', 'ascii', binary_folder)
    config_path = os.path.join(deployment_path, 'config', 'ngdac')
    nc_ngdac_path = os.path.join(deployment_path, 'data', 'out', 'nc', 'ngdac', mode)
    nc_trajectory_path = os.path.join(deployment_path, 'data', 'out', 'nc', 'trajectory', mode)

    tmp_path = os.path.join(deployment_path, 'data', 'tmp')
    pq_data_file = os.path.join(tmp_path, deployment_mode + '-data.parquet')
    pq_profiles_file = os.path.join(tmp_path, deployment_mode + '-profiles.parquet')
    
    # This is for GCP because buckets don't do implicit directories well on upload
    if not os.path.exists(tmp_path):
        logging.info('Creating directory at: {:}'.format(tmp_path))
        os.makedirs(tmp_path)
        
    if save_trajectory and (not os.path.exists(nc_trajectory_path)):
        logging.info('Creating directory at: {:}'.format(nc_trajectory_path))
        os.makedirs(nc_trajectory_path)
        
    if save_ngdac and (not os.path.exists(nc_ngdac_path)):
        logging.info('Creating directory at: {:}'.format(nc_ngdac_path))
        os.makedirs(nc_ngdac_path)


    # ### Read dba files - not necessary
    # logging.info('Getting dba files from: {:}'.format(ascii_path))
    # dba_files = get_dbas(ascii_path)
    # # logging.info('dba file info: {:}'.format(dba_files.info()))

    ### Create and process gdm object
    if not os.path.exists(config_path):
        logging.error('The config path does not exist {:}'.format(config_path))
        return 
                        
    gdm = GliderDataModel(config_path)
    if load_from_tmp:        
        logging.info('Loading gdm data from parquet files in: {:}'.format(tmp_path))
        gdm.data = pd.read_parquet(pq_data_file)
        gdm.profiles = pd.read_parquet(pq_profiles_file)
        logging.info('gdm from parquet files:\n {:}'.format(gdm))

    else:    
        logging.info('Creating GliderDataModel object from configs: {:}'.format(config_path))
        gdm = GliderDataModel(config_path)
    
        # Add data from dba files to gdm
        dba_files_list = list(map(lambda x: os.path.join(ascii_path, x), os.listdir(ascii_path)))
        dba_files = pd.DataFrame(dba_files_list, columns = ['dba_file'])
        
        logging.info('Reading ascii data into gdm object using {:} cores'.format(num_cores))
        if num_cores > 1:
            # If num_cores is greater than 1, run load_slocum_dba in parallel
            pool = mp.Pool(num_cores)
            load_slocum_dba_list = pool.map(load_slocum_dba, dba_files_list)
            pool.close()   
            
            load_slocum_dba_list_unzipped = list(zip(*load_slocum_dba_list))
            dba = pd.concat(load_slocum_dba_list_unzipped[0]).sort_index()
            pro_meta = pd.concat(load_slocum_dba_list_unzipped[1]).sort_index()            
            
            gdm.data = dba 
            gdm.profiles = pro_meta

        else :        
            # If num_cores == 1, run load_slocum_dba in normal for loop
            for index, row in dba_files.iterrows():
                # dba_file = os.path.join(row['path'], row['file'])
                dba, pro_meta = load_slocum_dba(row['dba_file'])
                
                gdm.data = pd.concat([gdm.data, dba])
                gdm.profiles = pd.concat([gdm.profiles, pro_meta])
            
        logging.info('gdm with data and profiles from dbas:\n {:}'.format(gdm))
    
        logging.info('Writing gdm to parquet files')
        gdm.data.to_parquet(pq_data_file, version="2.6", index = True)
        gdm.profiles.to_parquet(pq_profiles_file, version="2.6", index = True)

    # Remove garbage data, if specified
    if remove_19700101:
        row_count_orig = len(gdm.data.index)
        gdm.data = gdm.data[gdm.data.index != '1970-01-01']
        num_records_diff = len(gdm.data.index) - row_count_orig
        logging.info('Removed {:} invalid timestamps of 1970-01-01'.format(num_records_diff))

        
    # Make columns lowercase to match gdm behavior
    logging.info('Making sensor (data column) names lowercase to match gdm behavior')
    gdm.data.columns = gdm.data.columns.str.lower()


    ### Convert to time series, and write to nc file
    if save_trajectory:
        logging.info("Creating timeseries")
        ds = gdm.to_timeseries_dataset()
        
        logging.info("Writing timeseries to nc file")
        ds.to_netcdf(os.path.join(nc_trajectory_path, deployment_mode + '-trajectory.nc'))


    ### Write individual (profile) nc files
    # TODO: make parallel, when applicable
    if save_ngdac:
        logging.info("Writing ngdac to nc files")
        glider = dba_files.iloc[0].file.split('_')[0]
        for profile_time, pro_ds in gdm.iter_profiles():
            nc_name = '{:}-{:}-{:}.nc'.format(glider, profile_time.strftime('%Y%m%dT%H%M'), mode)
            nc_path = os.path.join(nc_ngdac_path, nc_name)
            logging.info('Writing {:}'.format(nc_path))
            pro_ds.to_netcdf(nc_path)
        
        
    logging.info('amlr_gdm processing complete for {:}'.format(deployment_mode))
    return gdm



if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], 
                    sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], 
                    sys.argv[9], sys.argv[10]))
