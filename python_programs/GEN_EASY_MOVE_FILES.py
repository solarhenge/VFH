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

root_folder_gen = common.get_property_value(properties_file_name,'root_folder_gen')
print('root_folder_gen')
print(root_folder_gen)
common.find_folder_name(root_folder_gen)

import glob
import os
import shutil

def get_yyyy_mm_dd( s, first ):
    try:
        start = s.index( first ) + len( first )
        start = start - 1
        yyyy_mm_dd = s[start:start+10]
        return yyyy_mm_dd
    except ValueError:
        return ""

root = root_folder_gen
for fname in os.listdir(root):
    if os.path.isfile(os.path.join(root, fname)):
        print(fname)

        #2127423_MANITOBA_LTD+EASY_DRIVER+2018-07-31.csv

        yyyy_mm_dd = get_yyyy_mm_dd(fname,"+2")
        print("yyyy_mm_dd")
        print(yyyy_mm_dd)
        #sys.exit()

        yyyy_mm_dd_dir = os.path.join(root, yyyy_mm_dd)

        if not os.path.isdir(yyyy_mm_dd_dir):
            os.mkdir(yyyy_mm_dd_dir)

        old_path_fname = root
        old_path_fname = os.path.join(root, fname)
        print('old_path_fname '+old_path_fname)

        new_path_fname = os.path.join(yyyy_mm_dd_dir, fname)
        print('new_path_fname '+new_path_fname)
        #sys.exit()

        shutil.move(old_path_fname, new_path_fname)
        logger1 = logging.getLogger(sys.argv[0]+' '+fname)
        logger1.info(old_path_fname+' moved to '+new_path_fname)


logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')
