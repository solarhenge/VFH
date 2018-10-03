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

root_folder_success_trip = common.get_property_value(properties_file_name,'root_folder_success_trip')
print('root_folder_success_trip')
print(root_folder_success_trip)
common.find_folder_name(root_folder_success_trip)

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
        sql_server_connection.commit()
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

import json

old_header = ['CUSTOMER','MONTH_END',
'PTP- Accessible Dispatch Vehicle Count',
'PTP- Accessible Dispatch Trip Count',
'PTP- Luxury-Accessible Dispatch Vehicle Count',
'PTP- Luxury-Accessible Dispatch Trip Count',
'PTP- Luxury-Standard Dispatch Vehicle Count',
'PTP- Luxury-Standard Dispatch Trip Count',
'PTP- Standard Dispatch Vehicle Count',
'PTP- Standard Dispatch Trip Count',
'TAXI Accessible Dispatch Vehicle Count',
'TAXI Accessible Dispatch Trip Count',
'TAXI Accessible Hail Vehicle Count',
'TAXI Accessible Hail Trip Count',
'TAXI Standard Dispatch Vehicle Count',
'TAXI Standard Dispatch Trip Count',
'TAXI Standard Hail Vehicle Count',
'TAXI Standard Hail Trip Count',
'UPLOADEDON','CREATEDON']
print('old_header')
print(old_header)
new_header = json.dumps(old_header)
new_header = new_header.replace('[',"")
new_header = new_header.replace(']',"")
new_header = new_header.replace('"',"")
print('new_header')
print(new_header)

def transform_column_values(column_values):
    print("transform_column_values begin")
    print("column_values")
    print(column_values)
    new_column_values = json.dumps(column_values)
    new_column_values = new_column_values.replace("'","")
    new_column_values = new_column_values.replace('"',"")
    new_column_values = new_column_values.replace('[',"")
    new_column_values = new_column_values.replace(']',"")
    new_column_values = new_column_values.replace(', ',',')
    print("new_column_values")
    print(new_column_values)
    print("transform_column_values end")
    return new_column_values

from datetime import date, datetime
import csv

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

sql = "SELECT [CUSTOMER],[MONTH_END],[CAR_COUNT_COLUMN_NAME],[TRIP_COUNT_COLUMN_NAME],[CAR_COUNT],[TRIP_COUNT],[UPLOADEDON],[CREATEDON] FROM [%s].[DBO].[VW_UPLOAD_EASY_TRIP_TRANSPOSE] ORDER BY [CUSTOMER],[MONTH_END],[CAR_COUNT_COLUMN_NAME]" % (database_vfh)
print("sql")
print(sql)
sql_server_cursor.execute(sql)

column_values = [None] * 20
count_t = [0] * 8
count_v = [0] * 8

old_company = 'old_company'
old_createdon = 'old_createdon'
old_month_end = 'old_month_end'
old_uploadedon = 'old_uploadedon'

def pop_count(row):
    if row[2] == 'P A D C':
        count_v[0] = row[4]
        count_t[0] = row[5]
    if row[2] == 'P L A D C':
        count_v[1] = row[4]
        count_t[1] = row[5]
    if row[2] == 'P L S D C':
        count_v[2] = row[4]
        count_t[2] = row[5]
    if row[2] == 'P S D C':
        count_v[3] = row[4]
        count_t[3] = row[5]
    if row[2] == 'T A D C':
        count_v[4] = row[4]
        count_t[4] = row[5]
    if row[2] == 'T A H C':
        count_v[5] = row[4]
        count_t[5] = row[5]
    if row[2] == 'T S D C':
        count_v[6] = row[4]
        count_t[6] = row[5]
    if row[2] == 'T S H C':
        count_v[7] = row[4]
        count_t[7] = row[5]
    print(row)

def pop_column_values():
    column_values[2] = count_v[0]
    column_values[3] = count_t[0]
    column_values[4] = count_v[1]
    column_values[5] = count_t[1]
    column_values[6] = count_v[2]
    column_values[7] = count_t[2]
    column_values[8] = count_v[3]
    column_values[9] = count_t[3]
    column_values[10] = count_v[4]
    column_values[11] = count_t[4]
    column_values[12] = count_v[5]
    column_values[13] = count_t[5]
    column_values[14] = count_v[6]
    column_values[15] = count_t[6]
    column_values[16] = count_v[7]
    column_values[17] = count_t[7]

import os
import time
fname = root_folder_success_trip+os.path.splitext(sys.argv[0])[0]+'+'+time.strftime("%Y%m%d_%H%M%S")+'.csv'
print(fname)
#sys.exit()
f = open(fname,'w')
f.write(new_header)
f.write('\n')

for row in sql_server_cursor:
    column_values = [None] * 20
    count_t = [0] * 8
    count_v = [0] * 8
    old_company = row[0]
    old_createdon = str(row[7])
    old_month_end = str(row[1])
    old_uploadedon = str(row[6])
    pop_count(row)
    column_values[0] = old_company
    column_values[1] = old_month_end
    pop_column_values()
    column_values[18] = old_uploadedon
    column_values[19] = old_createdon
    column_values = transform_column_values(column_values)
    f.write(column_values)
    f.write('\n')

f.close()
sql_server_connection.close()
logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')

