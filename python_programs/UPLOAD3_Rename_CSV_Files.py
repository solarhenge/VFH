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

root_folder_upload = common.get_property_value(properties_file_name,'root_folder_upload')
print('root_folder_upload')
print(root_folder_upload)
common.find_folder_name(root_folder_upload)

#SQL Server begin
sql_server_connect_string = common.get_property_value(properties_file_name,'sql_server_connect_string')
print('sql_server_connect_string')
print(sql_server_connect_string)

database_vfh = common.get_property_value(properties_file_name,'database_vfh')
print('database_vfh')
print(database_vfh)

import pyodbc
try:
    sql_server_connection = pyodbc.connect(sql_server_connect_string)
except pyodbc.Error as ex:
    #sqlstate = ex.args[0]
    print(ex)
    logger2 = logging.getLogger(sql_server_connect_string)
    logger2.error(ex)
    sys.exit()
sql_server_cursor = sql_server_connection.cursor()

def execute_sql_server(sql):
    try:
        sql_server_cursor.execute(sql)
    except pyodbc.DataError as e:
        data_error = True
        #sqlstate = e.args[0]
        print(e)
        logger2 = logging.getLogger(sys.argv[0]+' '+sql)
        logger2.error(e)
        #sys.exit()
    except pyodbc.ProgrammingError as e:
        data_error = True
        #sqlstate = e.args[0]
        print(e)
        logger2 = logging.getLogger(sys.argv[0]+' '+sql)
        logger2.error(e)
        #sys.exit()
#SQL Server end

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

def search(values, searchFor):
    ndx = 0
    for x in values:
        ndx = ndx+1
        if x == searchFor:
            return(ndx-1)
    return None

from datetime import datetime

from dateutil.parser import parse

def convert_cowboy_dates(s):
    print("convert_cowboy_dates")
    print("s = "+str(s))
    try:
        new_date = parse(s)
        print("new_date")
        print(new_date)
    except ValueError as e:
        print(e)
        if logger:
            logger2 = logging.getLogger(sys.argv[0]+' '+fname)
            logger2.error(e)
        return "ERROR"
    yyyy_mm_dd = str(new_date)[:10]
    print("yyyy_mm_dd")
    print(yyyy_mm_dd)
    yyyymmdd = yyyy_mm_dd.replace('-','')
    print("yyyymmdd")
    print(yyyymmdd)
    return yyyymmdd

def get_month_end(od):
    keys = list(od.keys())
    values = list(od.values())
    print("keys")
    print(keys)
    print("values")
    print(values)
    month_end_ndx = search(keys, 'month end (mm/dd/yyyy)')
    if month_end_ndx != None:
        month_end = values[month_end_ndx]
        month_end = str(month_end.strip())
        print("month_end")
        print(month_end)
        yyyymmdd = convert_cowboy_dates(month_end)
        if yyyymmdd == "ERROR":
            return "None"
        return yyyymmdd
    return "None"

from collections import OrderedDict
import csv
import glob
import os
import re

report_name_list = ['EASY_TRIP','EASY_DRIVER','EASY_VEHICLE']

extension = '.csv'

for root, dirs, files in os.walk(root_folder_upload):
    #print("root "+ root)
    #print("dirs ")
    #print(dirs)
    #print("files ")
    #print(files)

    if 'ETL' in root:
        print('root '+root)
        for fname in files:
            virgin = True
            old_month_end = "old_month_end"
            new_month_end = "new_month_end"
            print('fname '+fname)
            if os.path.splitext(fname)[-1] == extension:
                if not any(ext in fname for ext in report_name_list):
                    #print('report_name_list')
                    #print(report_name_list)
            
                    company_name_dir = root
                    print("company_name_dir "+company_name_dir)
                    company_name_dir_fname = os.path.join(root, fname) 
                    print("company_name_dir_fname "+company_name_dir_fname)

                    logger1 = logging.getLogger(sys.argv[0]+' '+fname)
                    logger1.info(company_name_dir_fname+' begin processing...')

                    key_found = False

                    #with open(company_name_dir_fname, encoding='utf-8') as f:
                    with open(company_name_dir_fname) as f:
                        try:
                            for row in csv.DictReader(f):
                                #print("row ")
                                #print(row)
                                od = OrderedDict(row)
                                #print("od ")
                                #print(od)
                                if ' vehicle count' in od.keys():
                                    key_found = True
                                    csv_filetype = 'EASY_TRIP'
                                    new_month_end = get_month_end(od)
                                    if new_month_end == "None":
                                        key_found = False

                                if ' drivers last name' in od.keys():
                                    key_found = True
                                    csv_filetype = 'EASY_DRIVER'
                                    new_month_end = get_month_end(od)
                                    if new_month_end == "None":
                                        key_found = False

                                if ' license plate' in od.keys():
                                    key_found = True
                                    csv_filetype = 'EASY_VEHICLE'
                                    new_month_end = get_month_end(od)
                                    if new_month_end == "None":
                                        key_found = False

                                if 'vehicle count' in od.keys():
                                    key_found = True
                                    csv_filetype = 'EASY_TRIP'
                                    new_month_end = get_month_end(od)
                                    if new_month_end == "None":
                                        key_found = False

                                if 'drivers last name' in od.keys():
                                    key_found = True
                                    csv_filetype = 'EASY_DRIVER'
                                    new_month_end = get_month_end(od)
                                    if new_month_end == "None":
                                        key_found = False

                                if 'license plate' in od.keys():
                                    key_found = True
                                    csv_filetype = 'EASY_VEHICLE'
                                    new_month_end = get_month_end(od)
                                    if new_month_end == "None":
                                        key_found = False
                                
                                print("old_month_end "+old_month_end)
                                print("new_month_end "+new_month_end)
                                if virgin:
                                    virgin = False
                                    old_month_end = new_month_end
                                else:
                                    if old_month_end != new_month_end:
                                        key_found = False
                                        e = "old_month_end "+old_month_end+" != new_month_end "+new_month_end
                                        print(company_name_dir_fname)
                                        print(e)
                                        logger2 = logging.getLogger(sys.argv[0]+' '+fname)
                                        logger2.error(e)

                        except UnicodeDecodeError as e:
                            key_found = False
                            print(company_name_dir_fname)
                            print(e)
                            logger2 = logging.getLogger(sys.argv[0]+' '+fname)
                            logger2.error(e)

                    print("company_name_dir_fname "+company_name_dir_fname)
                    suffix = 'SUFFIX'
                    if key_found:
                        suffix = 'SUCCESS'
                        first_part = company_name_dir_fname.split(".csv",1)[0]
                        print('first_part = '+first_part)
                        new_fname = first_part+"-"+csv_filetype+"+"+new_month_end+".csv"
                    else:
                        suffix = 'FAILURE'
                        new_fname = company_name_dir_fname+'.'+suffix
                    print('new_fname = '+new_fname)
                    #sys.exit()

                    try:
                        os.rename(company_name_dir_fname,new_fname)
                        logger1 = logging.getLogger(sys.argv[0]+' '+fname)
                        logger1.info(company_name_dir_fname+' has been renamed to '+new_fname)
                    except FileExistsError as e:
                        print(company_name_dir_fname)
                        print(e)
                        logger2 = logging.getLogger(sys.argv[0]+' '+fname)
                        logger2.error(e)

                    logger1 = logging.getLogger(sys.argv[0]+' '+fname)
                    logger1.info(company_name_dir_fname+' end processing...')
logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')
