import uuid
import shutil
import sys
import fileinput
import yaml


TEMPLATE_CONFIG='./wazeccp.cfg'
INSTANCE_CONFIG='./instance/wazeccp.cfg'

guid = uuid.uuid4()

print('Configure the instance config at {}'.format(INSTANCE_CONFIG))

shutil.copyfile(TEMPLATE_CONFIG, INSTANCE_CONFIG)

for line in fileinput.input((INSTANCE_CONFIG), inplace=True):
    if line.strip().startswith('GUID='):
        line = "GUID=\"{}\"\n".format(str(guid))
    sys.stdout.write(line)
    

print('Configure the cron.yaml with the GUID...')
cronjob = {}
cronjob['description'] = 'Refresh Case Studies'
cronjob['url'] = '/{}/cron'.format(guid)
cronjob['schedule'] = 'every 10 minutes'

cronfile = {}
cronfile['cron'] = []
cronfile['cron'].append(cronjob)

with open('cron.yaml', 'w') as cronconfig:
    yaml.dump(cronfile, cronconfig)