# 

import os
import logging
logging.getLogger().setLevel(logging.DEBUG)

from amlrgliders.utils import amlr_year_path 
import amlrgliders.process as amlrp


if __name__ == '__main__':
    deployment = 'amlr06-20221205'
    project = 'FREEBYRD'
    mode = 'delayed'
    deployments_path = os.path.join(
        'C:/SMW', 'Gliders_Moorings', 'Gliders', 'Glider-Data'
    )
    
    deployment_split = deployment.split('-')
    deployment_mode = f'{deployment}-{mode}'
    year = amlr_year_path(project, deployment_split)

    glider_path = os.path.join(
        deployments_path, project, year, deployment, 'glider'
    )

    numcores = 5
    chunksize = 6

    loadfromtmp = False
    write_trajectory = False
    
    gdm = amlrp.amlr_gdm(
        deployment, project, mode, glider_path, 
        numcores, chunksize, loadfromtmp
    )



