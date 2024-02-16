import os
import pandas as pd
import xarray as xr

path_gliderdata = "C:/SMW/Gliders_Moorings/Gliders/Glider-Data"
path_data = os.path.join(
    path_gliderdata, "FREEBYRD", "2022-23", "amlr06-20221205", 
    "glider", "data"
)


path_ngdac_profiles = os.path.join(
    path_data, "out", "nc", "ngdac", "delayed"
)
path_pq = os.path.join(path_data, "tmp")
os.listdir(path_pq)

pq_profiles_file = os.path.join(
    path_data, "tmp", "amlr06-20221205-delayed-profiles.parquet"
)
pq_data_file = os.path.join(
    path_data, "tmp", "amlr06-20221205-delayed-data.parquet"
)

traj_nc = os.path.join(
    path_data, "out", "nc", "trajectory", "amlr06-20221205-delayed-trajectory.nc"
)




prof_meta = pd.read_parquet(pq_profiles_file)
# prof_meta.to_csv("C:/Users/sam.woodman/Downloads/amlr06-20221205-profiles.csv")

# x = pd.read_parquet(pq_data_file)
x_nc = xr.open_dataset(traj_nc)
x_nc

# import matplotlib.pyplot as plt
# import plotly.graph_objects as go
import plotly.express as px


y = x_nc.sel(time=slice('2023-01-10', '2023-01-13'))
y.depth.plot()

x_nc.depth.plot()

df = pd.DataFrame({'depth': y.depth.values, 
                   'time2': y.time.values})

fig = px.line(df, x='time2', y="depth")
fig.show()
