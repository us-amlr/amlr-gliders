import os
import logging
import multiprocessing as mp
# import numpy as np
import pandas as pd
from itertools import repeat

from gdm import GliderDataModel
from gdm.gliders.slocum import load_slocum_dba
from amlrgliders.utils import amlr_interpolate

logger = logging.getLogger(__file__)


# The names of AMLR-relevant variables in gdm.data object
amlr_gdm_varnames = [
    'time', 'latitude', 'longitude', 
    'depth', 'm_depth', 'm_heading', 'm_pitch', 'm_roll', 
    "ilatitude", "ilongitude", 'idepth', 'impitch', 'imroll', 
    'sci_water_cond', 'density', 'sci_water_pressure', 
    'salinity', 'sci_water_temp',     
    'sci_flbbcd_bb_units', 'sci_flbbcd_chlor_units', 
    'sci_flbbcd_cdom_units', 'sci_flbbcd_therm', 
    'sci_oxy4_oxygen', 'sci_oxy4_saturation', 'sci_oxy4_temp', 
    # 'sci_ctd41cp_timestamp', 'ctd41cp_timestamp', 
    'm_final_water_vx', 'm_final_water_vy', 'c_wpt_lat', 'c_wpt_lon'
]

# The names of AMLR-relevant variables in timeseries object
amlr_ds_varnames = [
    'time', 'latitude', 'longitude', 
    'depth', 'm_depth', 'm_heading', 'm_pitch', 'm_roll', 
    'lat', 'lon', 'idepth', 'impitch', 'imroll', 
    'conductivity', 'density', 'pressure', 
    'salinity', 'temperature', 
    'beta700', 'chlorophyll_a', 'cdom', 'sci_flbbcd_therm', 
    'oxy4_oxygen', 'oxy4_saturation', 'oxy4_temp', 
    'm_final_water_vx', 'm_final_water_vy', 'c_wpt_lat', 'c_wpt_lon'
    ]


def amlr_gdm(deployment, project, mode, glider_path, 
             numcores=0, loadfromtmp=False, clobbertmp=False):
    """
    Create gdm object from dba files. 
    Note the data stored in the tmp files has not 
    removed 1970-01-01 timestamps or made column names lowercase. 

    Args:
        deployment (str): Deployment name, eg amlr##-YYYYmmdd
        project (str): Project name, eg FREEBYRD
        mode (str): deployment mode; delayed or rt
        glider_path (str): path to glider folder within deployment folder
        numcores (int, optional): Number of cores to use to read dba files. 
            Defaults to 0.
        loadfromtmp (bool, optional): Load gdm data and profiles from 
            temporary parquet files?. Defaults to False.
        clobbertmp (bool, optional): If they exist, should temporary 
            parquet files be clobbered? Defaults to False.

    Returns:
        gdm: gdm object
    """

    #--------------------------------------------
    # Check mode and deployment and set related variables
    if not (mode in ['delayed', 'rt']):
        logger.error("mode must be either 'delayed' or 'rt'")
        return

    deployment_split = deployment.split('-')
    deployment_mode = f'{deployment}-{mode}'
    if len(deployment_split[1]) != 8:
        logger.error("The deployment string format must be 'glider-YYYYmmdd'" +
                     ", eg amlr03-20220101")
        return
    else:
        logger.info(f'Processing glider data using gdm for deployment {deployment_mode}')


    #--------------------------------------------
    # Checks
    prj_list = ['FREEBYRD', 'REFOCUS', 'SANDIEGO']
    if not (project in prj_list):
        logger.error(f"project must be one of {', '.join(prj_list)}")
        return 
    
    # Handle script default value
    if numcores == 0:
        numcores = mp.cpu_count()

    if not (1 <= numcores and numcores <= mp.cpu_count()):
        logger.error(f'numcores must be between 1 and {mp.cpu_count()}')
        return 
    
    if not os.path.isdir(glider_path):
        logger.error(f'glider_path ({glider_path}) does not exist')
        return
    logger.info(f'Glider deployment path: {glider_path}')


    #--------------------------------------------
    # Set path/file variables, and create file paths if necessary
    ascii_path  = os.path.join(glider_path, 'data', 'ascii', mode)
    config_path = os.path.join(glider_path, 'data', 'data-config')
    # nc_ngdac_path = os.path.join(glider_path, 'data', 'out', 'nc', 'ngdac', mode)
    # nc_trajectory_path = os.path.join(glider_path, 'data', 'out', 'nc', 'trajectory')

    tmp_path = os.path.join(glider_path, 'data', 'tmp')
    pq_data_file = os.path.join(tmp_path, f'{deployment_mode}-data.parquet')
    pq_profiles_file = os.path.join(tmp_path, f'{deployment_mode}-profiles.parquet')
    
    # Confirm direcotry exists as GCS buckets don't do implicit directories
    if not os.path.exists(tmp_path):
        logger.info(f'Creating directory at: {tmp_path}')
        os.makedirs(tmp_path)

    #--------------------------------------------
    # Create gdm object, and load in data
    if not os.path.exists(config_path):
        logger.error(f'The config path does not exist {config_path}')
        return 
                        
    logger.info(f'Creating GliderDataModel object from configs: {config_path}')
    gdm = GliderDataModel(config_path)
    
    if loadfromtmp:        
        logger.info(f'Loading gdm data and profiles from parquet files in: {tmp_path}')
        gdm.data = pd.read_parquet(pq_data_file)
        gdm.profiles = pd.read_parquet(pq_profiles_file)

    else:    
        gdm.data, gdm.profiles = amlr_load_dba(ascii_path, numcores)
        
        # Write data to parquet files, if specified
        if not clobbertmp and os.path.exists(pq_profiles_file):
            logger.info(f'The parquet file for gdm profiles {pq_profiles_file} ' + 
                        'already exists, and will not be clobbered')
        else:
            logger.info('Writing gdm profiles to parquet file')
            gdm.profiles.to_parquet(
                pq_profiles_file, version="2.6", index = True
            )

        if not clobbertmp and os.path.exists(pq_data_file):
            logger.info(f'The parquet file for gdm data {pq_data_file} ' + 
                        'already exists, and will not be clobbered')
        else:
            logger.info('Writing gdm data to parquet file')
            gdm.data.to_parquet(
                pq_data_file, version="2.6", index = True
            )
        
    #--------------------------------------------
    ### Additional processing of gdm object

    # Make columns lowercase to match sensor definitions yaml file
    logger.info('Making sensor (data column) names lowercase')
    gdm.data.columns = gdm.data.columns.str.lower()

    # Remove garbage data
    #   Removing these timestamps is for situations when there is a " + 
    #   'Not enough timestamps for yo interpolation' warning",
    if any(gdm.data.index == '1970-01-01'):
        n_toremove = sum(gdm.data.index == '1970-01-01')
        logger.info(f'Removing {n_toremove} invalid (1970-01-01) timestamps')
        gdm.data = gdm.data[gdm.data.index != '1970-01-01']
    else:
        logger.info('No invalid (1970-01-01) timestamps to remove')

    # Remove duplicate timestamps
    gdm_dup = gdm.data.index.duplicated(keep='last')
    if any(gdm_dup):
        logger.info('Removing duplicated timestamps')
        gdm.data = gdm.data[~gdm_dup]
        logger.info(f'Removed {gdm_dup.sum()} rows with duplicated timestamps')
    else:
        logger.info('No duplicated timestamps to remove')

    # Create interpolated variables
    logger.info('Creating interpolated variables')
    gdm.data['idepth']  = amlr_interpolate(gdm.data['depth'])
    gdm.data['imdepth'] = amlr_interpolate(gdm.data['m_depth'])
    gdm.data['impitch'] = amlr_interpolate(gdm.data['m_pitch'])
    gdm.data['imroll']  = amlr_interpolate(gdm.data['m_roll'])

    #--------------------------------------------
    logger.info(f'Returning gdm object')    
    logger.info(f'gdm:\n {gdm}')

    return gdm



def amlr_load_dba(ascii_path, numcores):
    """
    Read in dba files from ascii_path using numcores cores
    Returns dba data and profile data frames

    Args:
        ascii_path (str): path to ascii (dba) files
        numcores (int): number of cores to use
        
    Returns:
        Tuple of dba (data) and profiles data frames
    """
    dba_files_list = list(
        map(lambda x: os.path.join(ascii_path, x), os.listdir(ascii_path))
    )
    # dba_files = pd.DataFrame(dba_files_list, columns = ['dba_file'])
    # dba_files_count = len(dba_files.index)        
    logger.info(f'Reading ascii data from {len(dba_files_list)} files ' + 
                f'using {numcores} core(s)')

    if len(dba_files_list) == 0:
        logger.error(f'There are no dba files in the expected directory ' + 
            f'({ascii_path}), and thus the gdm object cannot be created')
        return
    
    # Read dba files
    if numcores > 1:
        logger.debug('Reading dba files in parallel')
        # If numcores is greater than 1, run load_slocum_dba in parallel
        with mp.Pool(numcores) as pool:
            load_slocum_dba_list = pool.map(load_slocum_dba, dba_files_list)
        # pool = mp.Pool(numcores)
        # load_slocum_dba_list = pool.map(load_slocum_dba, dba_files_list)
        # pool.close()
        # pool.join()
        
        logger.info('Zipping output and concatenating data')
        # dba_zip_list = list(zip(*load_slocum_dba_list))
        dba_zip, pro_meta_zip = zip(*load_slocum_dba_list)
        del load_slocum_dba_list, pool
                    
        logger.debug('Concatenating pool output into profile data frame')
        pro_meta_df = pd.concat(pro_meta_zip)
        
        logger.debug('Concatenating pool output into trajectory data frame')
        dba_df = pd.concat(dba_zip)
        del pro_meta_zip, dba_zip

    else :        
        logger.debug(f'Reading dba files in for loop')
        pro_meta_df = pd.DataFrame()
        dba_df = pd.DataFrame()
        for i in dba_files_list:
            logger.debug(f'dba file: {i}')
            dba, pro_meta = load_slocum_dba(i)                
            pro_meta_df = pd.concat([pro_meta_df, pro_meta])
            dba_df = pd.concat([dba_df, dba])
        
    logger.info('Sorting data and profile data frames by time index')
    pro_meta_df = pro_meta_df.sort_index()
    dba_df = dba_df.sort_index()
   
    logger.info('Returning data and profiles data frames')    
    return dba_df, pro_meta_df


def amlr_write_trajectory(gdm, deployment, mode, glider_path, write_full = True):
    """
    From gdm file, write trajectory two nc files, 
    one with commonly used variables and the other with all variables
    
    Args:
        gdm (GliderDataModel): gdm object created by amlr_gdm
        deployment (str): deployment string, eg amlr##-YYYYmmdd
        mode (str): mode string, eg delayed
        glider_path (str): path to glider folder
        
    Returns: 0
    """

    deployment_mode = f'{deployment}-{mode}'
    nc_trajectory_path = os.path.join(glider_path, 'data', 'nc', 'trajectory')

    if not os.path.exists(nc_trajectory_path):
            logger.info(f'Creating directory at: {nc_trajectory_path}')
            os.makedirs(nc_trajectory_path)

    logger.info("Creating full timeseries")
    ds = gdm.to_timeseries_dataset()

    # # Note: to_timeseries_dataset uses nc_var_name in sensor_defs,
    # #   hence it changes ilatitude to lat and ilongitude to lon 
    # vars_list = ['time', 'latitude', 'longitude', 
    #     'depth', 'm_depth', 'm_heading', 'm_pitch', 'm_roll', 
    #     'lat', 'lon', 'idepth', 'impitch', 'imroll', 
    #     'cdom', 'conductivity', 'density', 'pressure', 
    #     'salinity', 'temperature', 'beta700', 'chlorophyll_a', 
    #     'oxy4_oxygen', 'oxy4_saturation', 
    #     'oxy4_temp', 'sci_flbbcd_therm', 'ctd41cp_timestamp', 
    #     'm_final_water_vx', 'm_final_water_vy', 'c_wpt_lat', 'c_wpt_lon']

    subset = sorted(set(amlr_ds_varnames).intersection(list(ds.keys())), 
                    key = amlr_ds_varnames.index)
    logger.debug(f"Length of vars_list: {len(amlr_ds_varnames)}")
    logger.debug(f"Number of variables in subset: {len(subset)}")
    ds_subset = ds[subset]
    
    logger.info("Writing trajectory timeseries for most commonly used variables to nc file")
    try:
        ds_subset.to_netcdf(
            os.path.join(nc_trajectory_path, f'{deployment_mode}-trajectory.nc'))
        logger.info("Subset trajectory timeseries written to nc file")
    except:
        logger.warning("Unable to write subset trajectory timeseries to nc file")

    if write_full:
        logger.info("Writing full trajectory timeseries to nc file")
        try:
            ds.to_netcdf(os.path.join(
                nc_trajectory_path, f'{deployment_mode}-trajectory-full.nc'
            ))
            logger.info("Full trajectory timeseries written to nc file")
        except:
            logger.warning("Unable to write full trajectory timeseries to nc file")
    else:
        logger.info("Not trying to write full trajectory timeseries to nc file")
        
    return 0


def amlr_write_ngdac(gdm, deployment, mode, nc_path):
    """
    From gdm object, 
    
    Args:
        gdm (GliderDataModel): gdm object
        deployment (str): deployment string, eg amlr##-YYYYmmdd
        mode (str): mode string, eg delayed
        nc_path (str): path to which to write the profile nc files

    Returns: 0
    """
    
    # nc_ngdac_path = os.path.join(glider_path, 'data', 'out', 'nc', 'ngdac', mode)
    logger.debug(f'NGDAC nc path: {nc_path}')
    if not os.path.exists(nc_path):
        logger.info(f'Creating directory at: {nc_path}')
        os.makedirs(nc_path)

    subset = sorted(set(amlr_gdm_varnames).intersection(list(gdm.data.columns)), 
                        key = amlr_gdm_varnames.index)
    gdm.data = gdm.data[subset]
    
    # TODO: make parallel?
    # if numcores > 1:
    #     logger.info("Writing ngdac to nc files, in parallel")    
    #     def write_ngdac_file(profile_time, row, pro_ds):
    #         pro_ds['profile_direction'] = row.direction
    #         nc_file = os.path.join(
    #             nc_path, 
    #             f"{test}_{profile_time.strftime('%Y%m%dT%H%M%S')}_{test}.nc"
    #         )
    #         pro_ds.to_netcdf(nc_file)
                        
    #     with mp.Pool(numcores) as pool:
    #         pool.starmap(
    #             write_ngdac_file, gdm.iter_profiles()
    #             # zip(gdm.iter_profiles(), repeat(deployment), repeat(mode))
    #         )

    # else:
    # NOTE: requires local gdm install with altered iter_profiles
    logger.info(f"Writing {len(gdm.profiles.index)} ngdac nc files, serially")
    for profile_time, row, pro_ds in gdm.iter_profiles():
        logger.debug(f"profile time: {profile_time}")
        pro_ds['profile_direction'] = row.direction

        nc_name = f"{deployment}_{profile_time.strftime('%Y%m%dT%H%M%S')}_{mode}.nc"
        nc_file = os.path.join(nc_path, nc_name)
        logger.info('Writing {:}'.format(nc_file))
        pro_ds.to_netcdf(nc_file)
            
    return 0
