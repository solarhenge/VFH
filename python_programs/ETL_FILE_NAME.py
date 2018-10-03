#common code begin
import sys
argument_count = 2

if len(sys.argv) < argument_count:
    print('expecting '+str(argument_count-1)+' arguments')
    sys.exit()

python_filename = sys.argv[0]
print('python_filename')
print(python_filename)
environment = sys.argv[1]
print('environment')
print(environment)

import common
common.check_environment(environment)

properties_file_name = common.get_properties_file_name(environment)
print('properties_file_name')
print(properties_file_name)

log_folder = common.get_property_value(properties_file_name,'log_folder')
print('log_folder')
print(log_folder)
common.find_folder_name(log_folder)

logging_filename = common.get_logging_filename(python_filename,log_folder)
print('logging_filename')
print(logging_filename)

# set up logging to file
import logging
import datetime as dt

class MyFormatter(logging.Formatter):
    converter=dt.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)
        return s

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console = logging.StreamHandler()
logger.addHandler(console)
formatter = MyFormatter(fmt='%(asctime)s %(message)s',datefmt='%Y-%m-%d_%H:%M:%S.%f')
console.setFormatter(formatter)

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    filename=logging_filename,
                    filemode='w')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
#formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
#console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)
logger1 = logging.getLogger(sys.argv[0])
logger1.info('begin processing...')
#common code end

root_folder_etl = common.get_property_value(properties_file_name,'root_folder_etl')
print('root_folder_etl')
print(root_folder_etl)
common.find_folder_name(root_folder_etl)

root_folder_upload = common.get_property_value(properties_file_name,'root_folder_upload')
print('root_folder_upload')
print(root_folder_upload)
common.find_folder_name(root_folder_upload)

from collections import OrderedDict
import glob
import os
import time

fname = root_folder_etl+os.path.splitext(sys.argv[0])[0]+'+'+time.strftime("%Y%m%d_%H%M%S")+'.csv'
print(fname)
#sys.exit()

f = open(fname,'w')
new_header = 'Name'
f.write(new_header)
f.write('\n')

with open(fname, 'w', newline='') as csvfile:
    for root, dirs, files in os.walk(root_folder_upload):
        if 'ETL' in root:
            print('root '+root)
            for fname in files:
                print("fname "+fname)
                f.write(fname)
                f.write('\n')
f.close()
logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')
