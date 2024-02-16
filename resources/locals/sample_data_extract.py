# Extract ~30 profiles from amlr06-20221205 to have available as a 'sample dataset'. 
# This is particularly for developing GLiderTools-related code

import os
import shutil
import pandas as pd

path_gliderdata = "C:/SMW/Gliders_Moorings/Gliders/Glider-Data"
path_ngdac_profiles = os.path.join(
    path_gliderdata, "FREEBYRD", "2022-23", "amlr06-20221205", 
    "glider", "data", "out", "nc", "ngdac", "delayed"
)
pq_profiles_file = os.path.join(
    path_gliderdata, "FREEBYRD", "2022-23", "amlr06-20221205", 
    "glider", "data", "tmp", "amlr06-20221205-delayed-profiles.parquet"
)

os.listdir(path_ngdac_profiles)



# See what to get 
prof_meta = pd.read_parquet(pq_profiles_file)


path_sample = "C:/SMW/Gliders_Moorings/Gliders/sample-profiles"
files_tocopy = os.listdir(path_ngdac_profiles)[203:230]
for i in files_tocopy:
    shutil.copy2(os.path.join(path_ngdac_profiles, i), 
                 os.path.join(path_sample, i))  
