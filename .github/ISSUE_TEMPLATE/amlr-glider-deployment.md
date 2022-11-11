---
name: AMLR Glider Deployment
about: Checklist for tasks associated with each AMLR glider deployment
title: amlr00-YYYYmmdd deployment
labels: ''
assignees: ''

---

Checklist for items 

# Before deployment
- [ ] Copy the [glider template folder](https://console.cloud.google.com/storage/browser/_details/amlr-gliders-deployments-dev/template-glider-YYYYmmdd.zip;tab=live_object?project=ggn-nmfs-usamlr-dev-7b99) into the correct project folder, and rename as applicable.
- [ ] Delete sensor folders (e.g., shadowgraph) if the glider does not have that specific sensor
- [ ] Add files to the 'deployment/prep' folder as necessary. TODO: specify files, add glider tasks like ballast and functional checkout?
- [ ] Add files to the ‘sensor/…/config’ folders as necessary. TODO: specify files
- [ ] Update the Glider&Mooring Database with device and glider build information

# During deployment
- [ ] Ensure the deployment folder name has the actual deployment date (e.g. amlr01-20220601)

# After deployment
