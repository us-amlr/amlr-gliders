echo "#!/bin/bash

gsutil cp gs://amlr-gliders-deployments-dev/SANDIEGO/2023/amlr04-20230620/scripts/amlr04-20230620-rt.sh /usr/local/bin/ 
chmod 755 -R /usr/local/bin" > /usr/local/bin/amlr04-20230620-cron.sh