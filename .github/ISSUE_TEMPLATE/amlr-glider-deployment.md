---
name: AMLR Glider Deployment
about: Checklist for tasks associated with each AMLR glider deployment
title: amlr00-YYYYmmdd deployment
labels: ''
assignees: ''

---

Checklist for AMLR glider deployment todo tasks. Note that file paths are relative to the top-level glider folder (i.e. glider-YYYYYmmdd)

# Before deployment
- [ ] Copy the [glider template folder](https://console.cloud.google.com/storage/browser/_details/amlr-gliders-deployments-dev/template-glider-YYYYmmdd.zip;tab=live_object?project=ggn-nmfs-usamlr-dev-7b99) into the correct project folder (FREEBYRD, REFOCUS, or SANDIEGO), and rename as applicable.
- [ ] Delete sensor folders (e.g., shadowgraph) if the glider does not have that specific sensor
- [ ] Add files to the 'deployment/prep' folder as necessary. TODO: specify files. 
- [ ] Todo: add glider tasks like ballast and functional checkout?
- [ ] Add files to the ‘sensor/…/config’ folders as necessary. TODO: specify files
- [ ] Update the Glider&Mooring Database with device and glider build information
- [ ] Copy default gdm config from [here](https://github.com/us-amlr/amlr-gliders/tree/main/resources/config-templates) into 'glider/config/gdm'. Edit deployment.yml, global_attributes.yml, and instruments.yml. In particular, update the instruments (names, serial numbers, and calibration dates) and the summary block in global_attributes.yml.

# During deployment
- [ ] Ensure the deployment folder name has the actual deployment date (e.g. amlr01-20220601)
- [ ] Ensure the deployment folder is in the glider deployments bucket ([amlr-gliders-deployments-dev](https://console.cloud.google.com/storage/browser/amlr-gliders-deployments-dev;tab=objects?project=ggn-nmfs-usamlr-dev-7b99))
- [ ] Update the Glider&Mooring Database with deployment information, and any last-minute changes

# After deployment
- [ ] Back up data: if applicable, put zipped copies of the Flight and Science folders in 'glider/backup'
- [ ] Populate the ‘glider/logs’ folder, either with a) all files from both sets of the LOGS and SENTLOGS folders (if doing a full download), or b) all of the files transferred over the air
- [ ] Copy the Cache files to [cache folder](https://console.cloud.google.com/storage/browser/amlr-gliders-deployments-dev/cache?project=ggn-nmfs-usamlr-dev-7b99) in the GCP bucket
- [ ] Copy all dbd and ebd files to ‘glider/data/in/binary/delayed
- [ ] Download desired comms (e.g., Event Timeline) from the SFMC to ‘docs/comms’
- [ ] If applicable, copy sensor config files to the various config directories in 'sensors'
- [ ] If applicable, copy AZFP data to ‘sensors/azfp/data/in/delayed’
- [ ] If applicable, copy glidercam data to the GCP [imagery bucket](https://console.cloud.google.com/storage/browser/amlr-imagery-raw-dev/gliders?project=ggn-nmfs-usamlr-dev-7b99)
- [ ] If applicable, copy Nortek data to ‘sensors/nortek/data/in/delayed’
- [ ] If applicable, copy shadowgraph images to the GCP [imagery bucket](https://console.cloud.google.com/storage/browser/amlr-imagery-raw-dev/gliders?project=ggn-nmfs-usamlr-dev-7b99)
- [ ] Update the Glider&Mooring Database with deployment information, and any last-minute changes

# Data processing
- [ ] Ensure that gdm config files at 'glider/config/gdm' are updated
- [ ] Run amlr_binary_to_dba.py script from amlr-gliders to convert binary data files to ascii (DBA) files
- [ ] Run amlr_process_dba_to_nc.py script from amlr-gliders with --write_trajectory flag to write trajectory NetCDF files
- [ ] If applicable, run amlr_process_dba_to_nc.py script from amlr-gliders with --write_acoustics and --write_imagery flags to write files for acoustic and imagery data processing
