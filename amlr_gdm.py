# -*- coding: utf-8 -*-
"""
Created on Wed Mar 16 10:15:26 2022

@author: sam.woodman
"""

import os
import sys
import pandas as pd
import logging
import pickle

sys.path.append("C:/SMW/Gliders_Moorings/Gliders/gdm")
from gdm import GliderDataModel
from gdm.gliders.slocum import get_dbas, load_slocum_dba


# TODO? either add code to make all variable names lowercase, or change gdm code

def amlr_gdm(project, deployment, mode, 
             load_from_pkl = False, remove_19700101 = False, 
             save_trajectory = True, save_ngdac = True):
    # Process AMLR glider data and save data to nc files.
    # This script depends requires the directory structure specified in the 
    #   AMLR glider data management readme
    # Returns gdm object
    
    # Parameters:
    # project: string; name of project. 
    #       Should be one of FREEBYRD, REFOCUS, or SANDIEGO
    # deployment: string; name of deployment, eg '20220308-amlr03'
    # mode: string; data mode must be either 'rt' or 'delayed'
    # Optional params:
    # load_from_pkl: boolean; indicates gdm object should be loaded from pkl file
    # remove_19700101: boolean; indicates if data with the timestamp 1970-01-01
    #   should be removed, before writing to pkl file. 
    #   Will be ignored if load_from_pkl = True
    #   For when there is a 'Not enough timestamps for yo interpolation' warning
    # save_trajectory: boolean; indicates if trajectory should be saved to a nc file
    # save_ngdac: boolean; indicates if profiles should be saved to nc files
            
            
    # logger = logging.getLogger()
    # logger.setLevel(logging.INFO)
    # console_handler = logging.StreamHandler()
    # logger.addHandler(console_handler)

    logging.basicConfig(level=logging.INFO)
    
    # Argument checks
    if not (project in ['FREEBYRD', 'REFOCUS', 'SANDIEGO']):
        logging.error("project must be one of 'FREEBYRD', 'REFOCUS', or 'SANDIEGO'")
        return 
    
    if not (mode in ['delayed', 'rt']):
        logging.error("mode must be either 'delayed' or 'rt'")
        return 
        
        
    if mode == 'delayed':
        binary_folder = 'dbd'
    else: 
        binary_folder = 'sbd'
        

    # Set path/file variables
    deployment_mode = deployment + '-' + mode
    prj_depl_path = os.path.join(project, deployment)

    ascii_path  = os.path.join(prj_depl_path, 'data', 'ascii', binary_folder)
    config_path = os.path.join(prj_depl_path, 'config', 'gdm')
    nc_ngdac_path = os.path.join(prj_depl_path, 'data', 'nc', 'ngdac', mode)
    nc_trajectory_path = os.path.join(prj_depl_path, 'data', 'nc', 'trajectory', mode)

    pkl_file_path = os.path.join(prj_depl_path, 'data', 'pkl', deployment_mode + '-gdm.pkl')


    # Read dba files
    logging.info('Getting dba files from: {:}'.format(ascii_path))
    dba_files = get_dbas(ascii_path)
    # logging.info('dba file info: {:}'.format(dba_files.info()))

    # Create gdm object
    if load_from_pkl:        
        logging.info('Loading GliderDataModel object from: {:}'.format(pkl_file_path))
        with open(pkl_file_path, 'rb') as inp:
            gdm = pickle.load(inp)
            
        logging.info('gdm from pkl file:\n {:}'.format(gdm))

    else:    
        logging.info('Creating GliderDataModel object from configs: {:}'.format(config_path))
        gdm = GliderDataModel(config_path)
    
        # Add data from dba files to gdm
        logging.info('Reading ascii data into gdm object')
        for index, row in dba_files.iterrows():
            dba_file = os.path.join(row['path'], row['file'])
            dba, pro_meta = load_slocum_dba(dba_file)
            
            gdm.data = pd.concat([gdm.data, dba])
            gdm.profiles = pd.concat([gdm.profiles, pro_meta])
            
        logging.info('gdm with data from dbas:\n {:}'.format(gdm))
    
        # Remove garbage data
        if remove_19700101:
            logging.info('Removing invalid timestamps')
            gdm.data = gdm.data[gdm.data.index != '1970-01-01']
    
        logging.info('Writing gdm to pkl file')
        with open(pkl_file_path, 'wb') as outp:
            pickle.dump(gdm, outp, pickle.HIGHEST_PROTOCOL)


    # Convert to time series, and write to nc file
    if save_trajectory:
        logging.info("Creating timeseries")
        ds = gdm.to_timeseries_dataset()
        
        logging.info("Writing timeseries to nc file")
        ds.to_netcdf(os.path.join(nc_trajectory_path, deployment_mode + '.nc'))


    # Write individual nc files
    if save_ngdac:
        logging.info("Writing ngdac to nc files")
        glider = dba_files.iloc[0].file.split('_')[0]
        for profile_time, pro_ds in gdm.iter_profiles():
            nc_path = os.path.join(nc_ngdac_path, '{:}_{:}_{:}.nc'.format(glider, profile_time.strftime('%Y%m%dT%H%M%S'), mode))
            logging.info('Writing {:}'.format(nc_path))
            pro_ds.to_netcdf(nc_path)
        
        
    logging.info('amlr_gdm processing complete for {:}'.format(deployment_mode))
    return gdm
