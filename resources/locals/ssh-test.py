import paramiko
import subprocess
from amlrgliders.scrape_sfmc import access_secret_version

gcpproject_id = 'ggn-nmfs-usamlr-dev-7b99'
secret_id = 'sfmc-swoodman'

ssh_client=paramiko.SSHClient()

myhostname = 'sfmc.webbresearch.com'
myusername = 'swoodman'
mypassword = access_secret_version(gcpproject_id, secret_id)

ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=myhostname, username=myusername, password=mypassword)

stdin,stdout,stderr=ssh_client.exec_command('ls \\var\\opt')
print(stdout.readlines())

ssh_client.close()