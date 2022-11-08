import os
import pathlib


def find_extensions(dir_path): #,  excluded = ['', '.txt', '.lnk']):
    """
    Get all the file extensions in the given directory
    From https://stackoverflow.com/questions/45256250
    """
    extensions = set()
    for _, _, files in os.walk(dir_path):   
        for f in files:
            extensions.add(pathlib.Path(f).suffix)
            # ext = Path(f).suffix.lower()
            # if not ext in excluded:
            #     extensions.add(ext)
    return extensions


def line_prepender(filename, line):
    """
    Title: prepend-line-to-beginning-of-a-file
    https://stackoverflow.com/questions/5914627
    """
    
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)


def amlr_year_path(project, deployment_split):
    """
    Generate and return the year string to use in file paths

    For the FREEBYRD project, this will be the Antarctic season, eg '2018-19'
    For all other projects, this will be a single year, eg '2018'
    """

    year = deployment_split[1][0:4]

    if project == 'FREEBYRD':
        month = deployment_split[1][4:6]
        if int(month) <= 7: 
            year = f'{int(year)-1}-{year[2:4]}'
        else:
            year = f'{year}-{str(int(year)+1)[2:4]}'

    return year


def solocam_filename_dt(filename, index_start):
    """
    Parse imagery filename to return associated datetime
    Requires index of start of datetime part of string-
    """
    solocam_substr = filename[index_start:(index_start+15)]
    solocam_dt = dt.datetime.strptime(solocam_substr, '%Y%m%d-%H%M%S')

    return solocam_dt
    