#!/usr/bin/env python

import os
import sys
import logging
import argparse
import copy

import pandas as pd
import datetime as dt
import glob
import math
import multiprocessing as mp
import numpy as np

import ipdb




# https://stackoverflow.com/questions/5914627/prepend-line-to-beginning-of-a-file
def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)


# def pd_interpolate_amlr(df):
#     return df.interpolate(method='time', limit_direction='forward', 
#                             limit_area='inside')



def amlr_acoustics(
    glider_path, deployment, mode, gdm, 
    pitch_column = 'ipitch', roll_column = 'iroll', depth_column = 'idepth', 
    lat_column = 'ilatitude', lon_column = 'ilongitude'
):
    """
    Create files for acoustics data processing
    """

    logging.info(f'Creating acoustics files for {deployment}')
    deployment_mode = f'{deployment}-{mode}'

    acoustics_path = os.path.join(glider_path, 'data', 'out', 'acoustics')
    if not os.path.exists(acoustics_path):
        logging.info(f'Creating directory at: {acoustics_path}')
        os.makedirs(acoustics_path)

    # gdm.data['idepth'] = utils.interpolate_timeseries(gdm.data.depth, gdm.data.index)
    # gdm.data['ipitch'] = utils.interpolate_timeseries(gdm.data.m_pitch, gdm.data.index)
    # gdm.data['iroll'] = utils.interpolate_timeseries(gdm.data.m_roll, gdm.data.index)

    gdm_dt_dt = gdm.data.index.values.astype('datetime64[s]').astype(dt.datetime)
    acoustic_file_pre = os.path.join(acoustics_path, deployment_mode)

    # Pitch
    logging.info(f'Creating Pitch file')
    pitch_dict = {'Pitch_date': [i.strftime('%m/%d/%Y') for i in gdm_dt_dt], 
                  'Pitch_time': [i.strftime('%H:%M:%S') for i in gdm_dt_dt], 
                  'Pitch_angle': [math.degrees(x) for x in gdm.data.ipitch]}
    pitch_df = pd.DataFrame(pitch_dict)
    pitch_df.to_csv(f'{acoustic_file_pre}-pitch.csv', index = False)


    # Roll
    logging.info(f'Creating Roll file')
    roll_dict = {'Roll_date': [i.strftime('%m/%d/%Y') for i in gdm_dt_dt],
                  'Roll_time': [i.strftime('%H:%M:%S') for i in gdm_dt_dt], 
                  'Roll_angle': [math.degrees(x) for x in gdm.data.iroll]}
    roll_df = pd.DataFrame(roll_dict)
    roll_df.to_csv(f'{acoustic_file_pre}-roll.csv', index = False)


    # GPS
    logging.info(f'Creating GPS file')
    gps_dict = {'GPS_date': [i.strftime('%Y-%m-%d') for i in gdm_dt_dt],
                  'GPS_time': [i.strftime('%H:%M:%S') for i in gdm_dt_dt], 
                  'Latitude': gdm.data.ilongitude, 
                  'Longitude': gdm.data.ilongitude}
    gps_df = pd.DataFrame(gps_dict)
    gps_df.to_csv(f'{acoustic_file_pre}-gps.csv', index = False)


    # Depth
    logging.info(f'Creating Depth file')
    depth_dict = {'Depth_date': [i.strftime('%Y%m%d') for i in gdm_dt_dt],
                  'Depth_time': [f"{i.strftime('%H%M%S')}0000" for i in gdm_dt_dt], 
                  'Depth': gdm.data.idepth, 
                  'repthree': 3}
    depth_df = pd.DataFrame(depth_dict)
    depth_file = f'{acoustic_file_pre}-depth.evl'
    depth_df.to_csv(depth_file, index = False, header = False, sep ='\t')
            
    line_prepender(depth_file, str(len(depth_df.index)))
    line_prepender(depth_file, 'EVBD 3 8.0.73.30735')

    logging.info(f'Completed creating acoustics files for {deployment}')
    return 0



def amlr_imagery(
    glider_path, deployment, ds, imagery_path, ext = 'jpg', 
    lat_column = 'lat', lon_column = 'lon', 
    depth_column = 'idepth', pitch_column = 'ipitch', roll_column = 'iroll'    
):
    """
    Match up imagery files with data from gdm object
    Returns dataframe with metadata information
    """
    
    logging.info(f'Creating imagery metadata file for {deployment}')

    #--------------------------------------------
    # Checks
    out_path = os.path.join(glider_path, 'data', 'out', 'cameras')
    if not os.path.exists(out_path):
        logging.info(f'Creating directory at: {out_path}')
        os.makedirs(out_path)

    if not os.path.isdir(imagery_path):
        logging.error(f'imagery_path ({imagery_path}) does not exist, and thus the ' + 
                        'CSV file with imagery metadata will not be created')
        return
    else:
        deployment_imagery_path = os.path.join(imagery_path, 'gliders', '2022', deployment)
        imagery_filepaths = glob.glob(f'{deployment_imagery_path}/**/*.{ext}', recursive=True)
        imagery_files = [os.path.basename(x) for x in imagery_filepaths]
        imagery_files.sort()

        # TODO: check for non-sensical file paths


    #--------------------------------------------
    # Extract info from imagery file names, and match up with glider data
    try:
        imagery_file_dts = [dt.datetime.strptime(i[5:20], '%Y%m%d-%H%M%S') for i in imagery_files]
    except:
        logging.error(f'Datetimes could not be extracted from imagery filenames ' + 
                        '(at {deployment_imagery_path}), and thus the ' + 
                        'CSV file with imagery metadata will not be created')
        return

    imagery_dict = {'img_file': imagery_files, 'img_dt': imagery_file_dts}
    imagery_df = pd.DataFrame(data = imagery_dict).sort_values('img_dt')

    ds_nona = ds.sel(time = ds.depth.dropna('time').time.values)
    ds_slice = ds_nona.sel(time=imagery_df.img_dt.values, method = 'nearest')

    imagery_df['glider_dt'] = ds_slice.time.values
    imagery_df['diff_dt_seconds'] = (imagery_df.img_dt - imagery_df.glider_dt).astype('timedelta64[s]').astype(np.int32)
    imagery_df['latitude'] = ds_slice[lat_column].values
    imagery_df['longitude'] = ds_slice[lon_column].values
    imagery_df['depth'] = ds_slice[depth_column].values
    imagery_df['pitch'] = ds_slice[pitch_column].values
    imagery_df['roll'] = ds_slice[roll_column].values

    csv_file = os.path.join(out_path, f'{deployment}-imagery-metadata.csv')
    logging.info(f'Writing imagery metadata CSV file ({csv_file})')
    imagery_df.to_csv(csv_file, index=False)

    imagery_df




def main(args):
    """
    Process raw AMLR glider data and write data to parquet and nc files.

    This script depends requires the directory structure specified in the 
    AMLR glider data management readme:
    https://docs.google.com/document/d/1X5DB4rQRBhBqnFdAAY_5Eyh_yPjG3UZ43Ga7ZGWcSsg

    Returns a gdm object. Note the gdm object in the tmp file has not 
    removed 1970-01-01 timestamps or made column names lowercase. 
    """

    #--------------------------------------------
    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)
    
    deployment = args.deployment
    project = args.project
    mode = args.mode
    deployments_path = args.deployments_path

    gdm_path = args.gdm_path
    numcores = args.numcores

    load_from_tmp = args.load_from_tmp
    keep_19700101 = args.keep_19700101
    write_trajectory = args.write_trajectory
    write_ngdac = args.write_ngdac
    
    write_acoustics = args.write_acoustics
    write_imagery = args.write_imagery
    imagery_path = args.imagery_path


    # Check mode and deployment and set related variables
    if not (mode in ['delayed', 'rt']):
        logging.error("mode must be either 'delayed' or 'rt'")
        return
    else:
        if mode == 'delayed':
            binary_type = 'debd'
        else: 
            binary_type = 'stbd'


    deployment_split = deployment.split('-')
    if len(deployment_split[1]) != 8:
        logging.error("The deployment string format must be 'glider-YYYYmmdd', eg amlr03-20220101")
        return

    else:
        logging.info(f'Processing glider data for deployment {deployment}, mode {mode}')


    # Append gdm path, and import functions
    if not os.path.isdir(gdm_path):
        logging.error(f'gdm_path ({gdm_path}) does not exist')
        return
    else:
        logging.info(f'Importing gdm functions from {gdm_path}')
        sys.path.append(gdm_path)
        from gdm import GliderDataModel
        from gdm.utils import interpolate_timeseries
        from gdm.gliders.slocum import load_slocum_dba #, get_dbas


    #--------------------------------------------
    # Checks
    prj_list = ['FREEBYRD', 'REFOCUS', 'SANDIEGO']
    if not (project in prj_list):
        logging.error(f"project must be one of {', '.join(prj_list)}")
        return 
    
    if not (1 <= numcores and numcores <= mp.cpu_count()):
        logging.error(f'numcores must be between 1 and {mp.cpu_count()}')
        return 
    
    if not os.path.isdir(deployments_path):
        logging.error(f'deployments_path ({deployments_path}) does not exist')
        return
    else:
        dir_expected = prj_list + ['cache']
        if not all(x in os.listdir(deployments_path) for x in dir_expected):
            logging.error(f"The expected folders ({', '.join(dir_expected)}) " + 
                f'were not found in the provided directory ({deployments_path}). ' + 
                'Did you provide the right path via deployments_path?')
            return 

    if write_imagery and mode == 'rt':
        logging.warning(f'You are processing imagery files using real-time data. ' + 
            'This may result in inaccurate imagery file metadata')


    #--------------------------------------------
    # Set path/file variables, and create file paths if necessary
    deployment_mode = f'{deployment}-{mode}'
    glider = deployment_split[0]
    year = deployment_split[1][0:4]

    glider_path = os.path.join(deployments_path, project, year, deployment, 'glider')
    logging.info(f'Glider path: {glider_path}')

    ascii_path  = os.path.join(glider_path, 'data', 'in', 'ascii', binary_type)
    config_path = os.path.join(glider_path, 'config', 'gdm')
    nc_ngdac_path = os.path.join(glider_path, 'data', 'out', 'nc', 'ngdac', mode)
    nc_trajectory_path = os.path.join(glider_path, 'data', 'out', 'nc', 'trajectory')

    tmp_path = os.path.join(glider_path, 'data', 'tmp')
    pq_data_file = os.path.join(tmp_path, f'{deployment_mode}-data.parquet')
    pq_profiles_file = os.path.join(tmp_path, f'{deployment_mode}-profiles.parquet')
    
    # This is for GCP because buckets don't do implicit directories well on upload
    if not os.path.exists(tmp_path):
        logging.info(f'Creating directory at: {tmp_path}')
        os.makedirs(tmp_path)
        
    if write_trajectory and (not os.path.exists(nc_trajectory_path)):
        logging.info(f'Creating directory at: {nc_trajectory_path}')
        os.makedirs(nc_trajectory_path)
        
    if write_ngdac and (not os.path.exists(nc_ngdac_path)):
        logging.info(f'Creating directory at: {nc_ngdac_path}')
        os.makedirs(nc_ngdac_path)


    #--------------------------------------------
    # # Read dba files - not necessary
    # logging.info('Getting dba files from: {:}'.format(ascii_path))
    # dba_files = get_dbas(ascii_path)
    # # logging.info('dba file info: {:}'.format(dba_files.info()))

    # Create and process gdm object
    if not os.path.exists(config_path):
        logging.error(f'The config path does not exist {config_path}')
        return 
                        
    logging.info(f'Creating GliderDataModel object from configs: {config_path}')
    gdm = GliderDataModel(config_path)
    if load_from_tmp:        
        logging.info(f'Loading gdm data from parquet files in: {tmp_path}')
        gdm.data = pd.read_parquet(pq_data_file)
        gdm.profiles = pd.read_parquet(pq_profiles_file)
        logging.info(f'gdm from parquet files:\n {gdm}')

    else:    
        logging.info(f'Reading ascii data into gdm object using {numcores} core(s)')
        # Add data from dba files to gdm
        dba_files_list = list(map(lambda x: os.path.join(ascii_path, x), os.listdir(ascii_path)))
        dba_files = pd.DataFrame(dba_files_list, columns = ['dba_file'])
        
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
            
        logging.info(f'gdm with data and profiles from dbas:\n {gdm}')

        logging.info('Sorting gdm data by time index')
        gdm.data.sort_index(inplace=True)
        gdm.profiles.sort_index(inplace=True)

        logging.info('Writing gdm to parquet files')
        gdm.data.to_parquet(pq_data_file, version="2.6", index = True)
        gdm.profiles.to_parquet(pq_profiles_file, version="2.6", index = True)

    # Remove garbage data, if specified
    if not keep_19700101:
        row_count_orig = len(gdm.data.index)
        gdm.data = gdm.data[gdm.data.index != '1970-01-01']
        num_records_diff = row_count_orig - len(gdm.data.index)
        logging.info(f'Removed {num_records_diff} invalid timestamps of 1970-01-01')

        
    # Make columns lowercase to match gdm behavior
    logging.info('Making sensor (data column) names lowercase to match gdm behavior')
    gdm.data.columns = gdm.data.columns.str.lower()

    # Create interpolated variables
    logging.info('Creating interpolated variables')
    # gdm_data_copy = gdm.data.copy()
    # gdm.data['idepth'] = pd_interpolate_amlr(gdm_data_copy.depth)
    # gdm.data['ipitch'] = pd_interpolate_amlr(gdm_data_copy.m_pitch)
    # gdm.data['iroll'] = pd_interpolate_amlr(gdm_data_copy.m_roll)
    gdm.data['idepth'] = gdm.data.depth.interpolate(method='time', limit_direction='forward', limit_area='inside')
    gdm.data['ipitch'] = gdm.data.m_pitch.interpolate(method='time', limit_direction='forward', limit_area='inside')
    gdm.data['iroll'] = gdm.data.m_roll.interpolate(method='time', limit_direction='forward', limit_area='inside')
    


    #--------------------------------------------
    # Convert to time series, and write to nc file
    if write_trajectory:
        logging.info("Creating timeseries")
        ds = gdm.to_timeseries_dataset()

        logging.info("Writing full trajectory timeseries to nc file")
        ds.to_netcdf(os.path.join(nc_trajectory_path, deployment_mode + '-trajectory-full.nc'))

        vars_list = ['time', 'lat', 'latitude', 'lon', 'longitude', 
            'depth', 'm_heading', 'm_pitch', 'm_roll', 
            'idepth', 'ipitch', 'iroll', 
            'cdom', 'conductivity', 'density', 'pressure', 
            'salinity', 'temperature', 'beta700', 'chlorophyll_a', 
            'oxy4_oxygen', 'oxy4_saturation', 
            'oxy4_temp', 'sci_flbbcd_therm', 'ctd41cp_timestamp', 
            'm_final_water_vx', 'm_final_water_vy', 'c_wpt_lat', 'c_wpt_lon']
        subset = sorted(set(vars_list).intersection(list(gdm.data.keys())), key = vars_list.index)

        ds_subset = ds[subset]
        logging.info("Writing trajectory timeseries for most commonly used variables to nc file")
        ds_subset.to_netcdf(os.path.join(nc_trajectory_path, deployment_mode + '-trajectory.nc'))


    # Write individual (profile) nc files
    # TODO: make parallel, when applicable
    if write_ngdac:
        logging.warning("CANNOT CURRENTLY WRITE TO NGDAC NC FILES")
        # logging.info("Writing ngdac to nc files")
        # glider = dba_files.iloc[0].file.split('_')[0]
        # for profile_time, pro_ds in gdm.iter_profiles():
        #     nc_name = '{:}-{:}-{:}.nc'.format(glider, profile_time.strftime('%Y%m%dT%H%M'), mode)
        #     nc_path = os.path.join(nc_ngdac_path, nc_name)
        #     logging.info('Writing {:}'.format(nc_path))
        #     pro_ds.to_netcdf(nc_path)


    # Write acoustics files
    if write_acoustics: 
        amlr_acoustics(glider_path, deployment, mode, gdm)

    # Write imagery metadata file
    if write_imagery:
        logging.info("Creating timeseries for imagery processing")

        imagery_vars_list = ['ilatitude', 'latitude', 'ilongitude', 'longitude', 
            'depth', 'density', 'm_heading', 'm_pitch', 'm_roll', 
            'idepth', 'ipitch', 'iroll']

        gdm_imagery = copy.deepcopy(gdm)        
        gdm_imagery.data = gdm_imagery.data[imagery_vars_list]
        ds_imagery = gdm_imagery.to_timeseries_dataset()
        amlr_imagery(glider_path, deployment, ds_imagery, imagery_path)
        
        
    logging.info('amlr_gdm processing complete for {:}'.format(deployment_mode))
    return gdm



if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__, 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter, 
        allow_abbrev=False)

    arg_parser.add_argument('deployment', 
        type=str,
        help='Deployment name, eg amlr03-20220425')

    arg_parser.add_argument('project', 
        type=str,
        help='Glider project name', 
        choices=['FREEBYRD', 'REFOCUS', 'SANDIEGO'])

    arg_parser.add_argument('mode', 
        type=str,
        help="Specify which binary files will be converted to dbas. " + 
            "'delayed' means [de]bd files will be converted, " + 
            "and 'rt' means [st]bd files will be converted", 
        choices=['delayed', 'rt'])

    arg_parser.add_argument('deployments_path', 
        type=str,
        help='Path to glider deployments directory. ' + 
            'In GCP, this will be the mounted bucket path')

    
    arg_parser.add_argument('--gdm_path', 
        type=str,
        help='Path to gdm module', 
        default='/opt/gdm')

    arg_parser.add_argument('--numcores',
        type=int,
        help='Number of cores to use when processing. ' + 
            'If greater than 1, parallel processing via mp.Pool.map will ' + 
            'be used for load_slocum_dbas and ' + 
            '(todo) writing individual (profile) nc files. ' +
            'This argument must be between 1 and mp.cpu_count()',
        default=1)

    arg_parser.add_argument('--load_from_tmp',
        help='flag; indicates gdm object should be loaded from ' + 
            'parquet files in glider/data/tmp directory',
        action='store_true')

    arg_parser.add_argument('--keep_19700101',
        help='flag; indicates if data with the timestamp 1970-01-01 '  + 
            'should be KEPT. Will be ignored if load_from_tmp is True. ' + 
            "Removing these timestamps is for situations when there is a " + 
            "'Not enough timestamps for yo interpolation' warning",
        action='store_true')

    arg_parser.add_argument('--write_trajectory',
        help='flag; indicates if trajectory nc file should be written',
        action='store_true')

    arg_parser.add_argument('--write_ngdac',
        help='flag; indicates if ngdac nc files should be written',
        action='store_true')

    arg_parser.add_argument('--write_acoustics',
        help='flag; indicates if acoustic files should be written',
        action='store_true')

    arg_parser.add_argument('--write_imagery',
        help='flag; indicates if imagery metadata csv file should be written',
        action='store_true')

    arg_parser.add_argument('--imagery_path',
        type=str,
        help='Path to imagery bucket',
        default='')

    arg_parser.add_argument('-l', '--loglevel',
        type=str,
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
