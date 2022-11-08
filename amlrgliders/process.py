import os
import logging
import copy
import datetime as dt
import glob
import math
import multiprocessing as mp
import numpy as np
import pandas as pd

from gdm import GliderDataModel
from gdm.gliders.slocum import load_slocum_dba #, get_dbas
# from gdm.utils import interpolate_timeseries

logger = logging.getLogger(__name__)


def amlr_interpolate(df, var_src, var_dst):
    if var_src in df.columns:
        logger.info(f'Creating interpolated data column ({var_dst}) from {var_src}')
        df[var_dst] = df[var_src].interpolate(method='time', limit_direction='forward', limit_area='inside')
    else:
        logger.info(f'No {var_src} variable, and thus {var_dst} will not be created')

    return df


def amlr_gdm(deployment, project, mode, glider_path, numcores, loadfromtmp):
    """
    Create gdm object from dba files. 
    Note the data stored in the tmp files has not 
    removed 1970-01-01 timestamps or made column names lowercase. 

    Returns gdm object
    """

    #--------------------------------------------
    # Check mode and deployment and set related variables
    if not (mode in ['delayed', 'rt']):
        logger.error("mode must be either 'delayed' or 'rt'")
        return

    deployment_split = deployment.split('-')
    deployment_mode = f'{deployment}-{mode}'
    if len(deployment_split[1]) != 8:
        logger.error("The deployment string format must be 'glider-YYYYmmdd', eg amlr03-20220101")
        return
    else:
        logger.info(f'Processing glider data using gdm for deployment {deployment_mode}')

    if not os.path.isdir(glider_path):
        logging.error(f'glider_path ({glider_path}) does not exist')
        return
    logging.info(f'Glider deployment path: {glider_path}')


    # # Append gdm path, and import functions
    # if not os.path.isdir(gdm_path):
    #     logger.error(f'gdm_path ({gdm_path}) does not exist')
    #     return
    # else:
    #     logger.info(f'Importing gdm functions from {gdm_path}')
    #     sys.path.append(gdm_path)
    #     # from gdm import GliderDataModel
    #     # from gdm.utils import interpolate_timeseries
    #     # from gdm.gliders.slocum import load_slocum_dba #, get_dbas


    #--------------------------------------------
    # Checks
    prj_list = ['FREEBYRD', 'REFOCUS', 'SANDIEGO']
    if not (project in prj_list):
        logger.error(f"project must be one of {', '.join(prj_list)}")
        return 
    
    if not (1 <= numcores and numcores <= mp.cpu_count()):
        logger.error(f'numcores must be between 1 and {mp.cpu_count()}')
        return 
    
    if not os.path.isdir(glider_path):
        logger.error(f'glider_path ({glider_path}) does not exist')
        return


    #--------------------------------------------
    # Set path/file variables, and create file paths if necessary
    ascii_path  = os.path.join(glider_path, 'data', 'in', 'ascii', mode)
    config_path = os.path.join(glider_path, 'config', 'gdm')
    nc_ngdac_path = os.path.join(glider_path, 'data', 'out', 'nc', 'ngdac', mode)
    nc_trajectory_path = os.path.join(glider_path, 'data', 'out', 'nc', 'trajectory')

    tmp_path = os.path.join(glider_path, 'data', 'tmp')
    pq_data_file = os.path.join(tmp_path, f'{deployment_mode}-data.parquet')
    pq_profiles_file = os.path.join(tmp_path, f'{deployment_mode}-profiles.parquet')
    
    # This is for GCP because buckets don't do implicit directories well on upload
    if not os.path.exists(tmp_path):
        logger.info(f'Creating directory at: {tmp_path}')
        os.makedirs(tmp_path)


    #--------------------------------------------
    # # Read dba files - not necessary
    # dba_files = get_dbas(ascii_path)

    # Create and process gdm object
    if not os.path.exists(config_path):
        logger.error(f'The config path does not exist {config_path}')
        return 
                        
    logger.info(f'Creating GliderDataModel object from configs: {config_path}')
    gdm = GliderDataModel(config_path)
    if loadfromtmp:        
        logger.info(f'Loading gdm data from parquet files in: {tmp_path}')
        gdm.data = pd.read_parquet(pq_data_file)
        gdm.profiles = pd.read_parquet(pq_profiles_file)
        logger.info(f'gdm from parquet files:\n {gdm}')

    else:    
        logger.info(f'Reading ascii data into gdm object using {numcores} core(s)')

        # Add data from dba files to gdm
        dba_files_list = list(map(lambda x: os.path.join(ascii_path, x), os.listdir(ascii_path)))
        dba_files = pd.DataFrame(dba_files_list, columns = ['dba_file'])

        if len(dba_files) == 0:
            logger.error(f'There are no dba files in the expected directory ' + 
                f'({ascii_path}), and thus the gdm object cannot be created')
            return
        
        if numcores > 1:
            # If numcores is greater than 1, run load_slocum_dba in parallel
            pool = mp.Pool(numcores)
            load_slocum_dba_list = pool.map(load_slocum_dba, dba_files_list)
            pool.close()   
            
            load_slocum_dba_list_unzipped = list(zip(*load_slocum_dba_list))
            dba = pd.concat(load_slocum_dba_list_unzipped[0]).sort_index()
            pro_meta = pd.concat(load_slocum_dba_list_unzipped[1]).sort_index()            
            
            gdm.data = dba 
            gdm.profiles = pro_meta

        else :        
            # If numcores == 1, run load_slocum_dba in normal for loop
            for index, row in dba_files.iterrows():
                # dba_file = os.path.join(row['path'], row['file'])
                dba, pro_meta = load_slocum_dba(row['dba_file'])
                
                gdm.data = pd.concat([gdm.data, dba])
                gdm.profiles = pd.concat([gdm.profiles, pro_meta])
            
        logger.info('Sorting gdm data by time index')
        gdm.data.sort_index(inplace=True)
        gdm.profiles.sort_index(inplace=True)

        logger.info('Writing gdm to parquet files')
        gdm.data.to_parquet(pq_data_file, version="2.6", index = True)
        gdm.profiles.to_parquet(pq_profiles_file, version="2.6", index = True)

        logger.info(f'gdm with data and profiles from dbas:\n {gdm}')

    #--------------------------------------------
    ### Additional processing of gdm object

    # Make columns lowercase to match gdm behavior
    logger.info('Making sensor (data column) names lowercase to match gdm behavior')
    gdm.data.columns = gdm.data.columns.str.lower()


    # Remove garbage data
    #   Removing these timestamps is for situations when there is a " + 
    #   'Not enough timestamps for yo interpolation' warning",
    if any(gdm.data.index != '1970-01-01'):
        logger.info('Removing invalid (1970-01-01) timestamps')
        row_count_orig = len(gdm.data.index)
        gdm.data = gdm.data[gdm.data.index != '1970-01-01']
        num_records_diff = row_count_orig - len(gdm.data.index)
        logger.info(f'Removed {num_records_diff} invalid timestamps of 1970-01-01')
    else:
        logger.info('No invalid (1970-01-01) timestamps to remove')

    # Remove duplicate timestamps
    gdm_dup = gdm.data.index.duplicated()
    if any(gdm_dup):
        logger.info('Removing duplicated timestamps')
        gdm.data = gdm.data[~gdm.data.index.duplicated(keep='last')]
        logger.info(f'Removed {gdm_dup.sum()} rows with duplicated timestamps')
    else:
        logger.info('No duplicated timestamps to remove')


    # Create interpolated variables
    logger.info('Creating interpolated variables')
    gdm.data = amlr_interpolate(gdm.data, 'depth', 'idepth')
    gdm.data = amlr_interpolate(gdm.data, 'm_depth', 'imdepth')
    gdm.data = amlr_interpolate(gdm.data, 'm_pitch', 'impitch')
    gdm.data = amlr_interpolate(gdm.data, 'm_roll', 'imroll')

    #--------------------------------------------
    logger.info('Returning gdm object')
    return gdm



def amlr_write_trajectory(gdm, deployment_mode, glider_path):
    """
    ...
    """

    nc_trajectory_path = os.path.join(glider_path, 'data', 'out', 'nc', 'trajectory')

    if not os.path.exists(nc_trajectory_path):
            logger.info(f'Creating directory at: {nc_trajectory_path}')
            os.makedirs(nc_trajectory_path)

    logger.info("Creating full timeseries")
    ds = gdm.to_timeseries_dataset()

    logger.info("Writing full trajectory timeseries to nc file")
    try:
        ds.to_netcdf(os.path.join(nc_trajectory_path, f'{deployment_mode}-trajectory-full.nc'))
        logger.info("Full trajectory timeseries written to nc file")
    except:
        logger.warning("Unable to write full trajectory timeseries to nc file")

    vars_list = ['time', 'lat', 'latitude', 'lon', 'longitude', 
        'depth', 'm_depth', 'm_heading', 'm_pitch', 'm_roll', 
        'idepth', 'impitch', 'imroll', 'ilatitude', 'ilongitude',
        'cdom', 'conductivity', 'density', 'pressure', 
        'salinity', 'temperature', 'beta700', 'chlorophyll_a', 
        'oxy4_oxygen', 'oxy4_saturation', 
        'oxy4_temp', 'sci_flbbcd_therm', 'ctd41cp_timestamp', 
        'm_final_water_vx', 'm_final_water_vy', 'c_wpt_lat', 'c_wpt_lon']
    subset = sorted(set(vars_list).intersection(list(gdm.data.keys())), key = vars_list.index)

    ds_subset = ds[subset]
    
    logger.info("Writing trajectory timeseries for most commonly used variables to nc file")
    try:
        ds_subset.to_netcdf(os.path.join(nc_trajectory_path, f'{deployment_mode}-trajectory.nc'))
        logger.info("Subset trajectory timeseries written to nc file")
    except:
        logger.warning("Unable to write subset trajectory timeseries to nc file")



def amlr_write_ngdac(gdm, deployment_mode, glider_path):
    """
    ...
    """
    # TODO: make parallel, once applicable

    nc_ngdac_path = os.path.join(glider_path, 'data', 'out', 'nc', 'ngdac', mode)

    # if not os.path.exists(nc_ngdac_path):
    #     logging.info(f'Creating directory at: {nc_ngdac_path}')
    #     os.makedirs(nc_ngdac_path)

    logging.error("CANNOT CURRENTLY WRITE TO NGDAC NC FILES")
    # logging.info("Writing ngdac to nc files")
    # glider = dba_files.iloc[0].file.split('_')[0]
    # for profile_time, pro_ds in gdm.iter_profiles():
    #     nc_name = '{:}-{:}-{:}.nc'.format(glider, profile_time.strftime('%Y%m%dT%H%M'), mode)
    #     nc_path = os.path.join(nc_ngdac_path, nc_name)
    #     logging.info('Writing {:}'.format(nc_path))
    #     pro_ds.to_netcdf(nc_path)
        


def amlr_acoustics(
    gdm, glider_path, deployment, mode,  
    pitch_column = 'ipitch', roll_column = 'iroll', depth_column = 'idepth', 
    lat_column = 'ilatitude', lon_column = 'ilongitude'
):
    """
    Create files for acoustics data processing
    """

    logger.info(f'Creating acoustics files for {deployment}')
    deployment_mode = f'{deployment}-{mode}'

    # Check that all required variables are present
    acoustic_vars_list = [pitch_column, roll_column, depth_column, lat_column, lon_column]
    acoustic_vars_set = set(acoustic_vars_list)
    if not acoustic_vars_set.issubset(gdm.data.columns):
        logger.error('gdm object does not contain all required columns. ' + 
            f"Missing columns: {', '.join(acoustic_vars_set.difference(gdm.data.columns))}")
        return()

    acoustics_path = os.path.join(glider_path, 'data', 'out', 'acoustics')
    if not os.path.exists(acoustics_path):
        logger.info(f'Creating directory at: {acoustics_path}')
        os.makedirs(acoustics_path)

    gdm_dt_dt = gdm.data.index.values.astype('datetime64[s]').astype(dt.datetime)
    acoustic_file_pre = os.path.join(acoustics_path, deployment_mode)
    
    logger.info(f'Writing acoustics files to {acoustics_path}')

    # Pitch
    logger.info(f'Creating Pitch file')
    pitch_dict = {'Pitch_date': [i.strftime('%m/%d/%Y') for i in gdm_dt_dt], 
                  'Pitch_time': [i.strftime('%H:%M:%S') for i in gdm_dt_dt], 
                  'Pitch_angle': [math.degrees(x) for x in gdm.data[pitch_column]]}
    pitch_df = pd.DataFrame(pitch_dict)
    pitch_df.to_csv(f'{acoustic_file_pre}-pitch.csv', index = False)


    # Roll
    logger.info(f'Creating Roll file')
    roll_dict = {'Roll_date': [i.strftime('%m/%d/%Y') for i in gdm_dt_dt],
                  'Roll_time': [i.strftime('%H:%M:%S') for i in gdm_dt_dt], 
                  'Roll_angle': [math.degrees(x) for x in gdm.data[roll_column]]}
    roll_df = pd.DataFrame(roll_dict)
    roll_df.to_csv(f'{acoustic_file_pre}-roll.csv', index = False)


    # GPS
    logger.info(f'Creating GPS file')
    gps_dict = {'GPS_date': [i.strftime('%Y-%m-%d') for i in gdm_dt_dt],
                  'GPS_time': [i.strftime('%H:%M:%S') for i in gdm_dt_dt], 
                  'Latitude': gdm.data[lat_column], 
                  'Longitude': gdm.data[lon_column]}
    gps_df = pd.DataFrame(gps_dict)
    gps_df.to_csv(f'{acoustic_file_pre}-gps.csv', index = False)


    # Depth
    logger.info(f'Creating Depth file')
    depth_dict = {'Depth_date': [i.strftime('%Y%m%d') for i in gdm_dt_dt],
                  'Depth_time': [f"{i.strftime('%H%M%S')}0000" for i in gdm_dt_dt], 
                  'Depth': gdm.data[depth_column], 
                  'repthree': 3}
    depth_df = pd.DataFrame(depth_dict)
    depth_file = f'{acoustic_file_pre}-depth.evl'
    depth_df.to_csv(depth_file, index = False, header = False, sep ='\t')
            
    line_prepender(depth_file, str(len(depth_df.index)))
    line_prepender(depth_file, 'EVBD 3 8.0.73.30735')

    logger.info(f'Completed creating acoustics files for {deployment}')
    return 0


def solocam_filename_dt(filename, index_start):
    """
    Parse imagery filename to return associated datetime
    Requires index of start of datetime part of string-
    """
    solocam_substr = filename[index_start:(index_start+15)]
    solocam_dt = dt.datetime.strptime(solocam_substr, '%Y%m%d-%H%M%S')

    return solocam_dt


def amlr_imagery(
    gdm, glider_path, deployment, imagery_path, ext = 'jpg', 
    lat_column = 'ilatitude', lon_column = 'ilongitude', 
    depth_column = 'idepth', pitch_column = 'impitch', roll_column = 'imroll'    
):
    """
    Matches up imagery files with data from gdm object by imagery filename
    Returns dataframe with metadata information
    """
    
    logger.info(f'Creating imagery metadata file for {deployment}')


    #--------------------------------------------
    # Checks
    out_path = os.path.join(glider_path, 'data', 'out', 'cameras')
    if not os.path.exists(out_path):
        logger.info(f'Creating directory at: {out_path}')
        os.makedirs(out_path)

    if not os.path.isdir(imagery_path):
        logger.error(f'imagery_path ({imagery_path}) does not exist, and thus the ' + 
                        'CSV file with imagery metadata will not be created')
        return
    else:
        deployment_imagery_path = os.path.join(imagery_path, 'gliders', '2022', deployment)
        imagery_filepaths = glob.glob(f'{deployment_imagery_path}/**/*.{ext}', recursive=True)
        imagery_files = [os.path.basename(x) for x in imagery_filepaths]
        imagery_files.sort()

        # TODO: check for non-sensical file paths\

    #--------------------------------------------
    logger.info("Creating timeseries for imagery processing")
    imagery_vars_list = ['latitude', 'longitude', 
        'depth', 'density', 'm_heading','m_depth', 'm_pitch', 'm_roll', 
        'ilatitude', 'ilongitude', 'idepth', 'impitch', 'imroll']
    imagery_vars_set = set(imagery_vars_list)

    if not imagery_vars_set.issubset(gdm.data.columns):
        logger.error('gdm object does not contain all required columns. ' + 
            f"Missing columns: {', '.join(imagery_vars_set.difference(gdm.data.columns))}")
        return()

    gdm.data = gdm.data[imagery_vars_list]
    ds = gdm.to_timeseries_dataset()


    #--------------------------------------------
    # Extract info from imagery file names, and match up with glider data
    logger.info("Processing imagery file names")

    # Check that all filenames have the same number of characters
    if not len(set([len(i) for i in imagery_files])) == 1:
        logger.error('The imagery file names are not all the same length, ' + 
            'and thus the imagery metadata file cannot be generated')
        return()


    space_index = str.index(imagery_files[0], ' ')
    if space_index == -1:
        logger.error('The imagery file name year index could not be found, ' + 
            'and thus the imagery metadata file cannot be generated')
        return()
    yr_index = space_index + 1   

    try:
        imagery_file_dts = [solocam_filename_dt(i, yr_index) for i in imagery_files]
    except:
        logger.error(f'Datetimes could not be extracted from imagery filenames ' + 
                        '(at {deployment_imagery_path}), and thus the ' + 
                        'CSV file with imagery metadata will not be created')
        return


    imagery_dict = {'img_file': imagery_files, 'img_dt': imagery_file_dts}
    imagery_df = pd.DataFrame(data = imagery_dict).sort_values('img_dt')

    logger.info("Finding nearest glider data slice for each imagery datetime")
    # ds_nona = ds.sel(time = ds.depth.dropna('time').time.values)
    # TODO: check if any time values are NA
    ds_slice = ds.sel(time=imagery_df.img_dt.values, method = 'nearest')

    imagery_df['glider_dt'] = ds_slice.time.values
    imagery_df['diff_dt_seconds'] = (imagery_df.img_dt - imagery_df.glider_dt).astype('timedelta64[s]').astype(np.int32)
    imagery_df['latitude'] = ds_slice[lat_column].values
    imagery_df['longitude'] = ds_slice[lon_column].values
    imagery_df['depth'] = ds_slice[depth_column].values
    imagery_df['pitch'] = ds_slice[pitch_column].values
    imagery_df['roll'] = ds_slice[roll_column].values

    csv_file = os.path.join(out_path, f'{deployment}-imagery-metadata.csv')
    logger.info(f'Writing imagery metadata CSV file to {csv_file}')
    imagery_df.to_csv(csv_file, index=False)

    imagery_df
