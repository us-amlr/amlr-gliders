import os
import logging
import datetime as dt
import math
import multiprocessing as mp
import pandas as pd

logger = logging.getLogger(__name__)


def line_prepender(filename, line):
    """
    Title: prepend-line-to-beginning-of-a-file
    https://stackoverflow.com/questions/5914627
    """
    
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)


def amlr_acoustics_metadata(gdm, deployment_mode, glider_path):
    """
    Create files for acoustics data processing, 
    using the interpolated variables
    
    Args:
        gdm (GliderDataModel): gdm object created by amlr_gdm
        glider_path (str): path to glider folder
        deployment (str): 
        mode (str): deployment-mode string, eg amlr##-YYYYmmdd-delayed
        
    Returns: 0
    """

    lat_column = 'ilatitude'
    lon_column = 'ilongitude'
    pitch_column = 'impitch'
    roll_column = 'imroll'
    depth_column = 'idepth' 

    logger.info(f'Creating acoustics files for {deployment_mode}')
    # deployment_mode = f'{deployment}-{mode}'

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

    logger.info(f'Acoustics files created for {deployment_mode}')
    return 0
