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

#SQL Server begin
sql_server_connect_string = common.get_property_value(properties_file_name,'sql_server_connect_string')
print('sql_server_connect_string')
print(sql_server_connect_string)

sql_server_database = common.get_property_value(properties_file_name,'sql_server_database')
print('sql_server_database')
print(sql_server_database)

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
#SQL Server end

report_name = "EASY_VEHICLE"

import json
old_header = ['month end (mm/dd/yyyy)','license plate','vin','vehicle make','vehicle model','year','vehicle type','dispatcher car id']
print('old_header')
print(old_header)
new_header = json.dumps(old_header)
new_header = new_header.replace('[',"")
new_header = new_header.replace(']',"")
new_header = new_header.replace('"',"")
print('new_header')
print(new_header)

def get_new_column_values(old_column_values):
    print("get_new_column_values begin")
    print("old_column_values")
    print(old_column_values)
    new_column_values = json.dumps(old_column_values)
    new_column_values = new_column_values.replace("'","")
    new_column_values = new_column_values.replace('"',"")
    new_column_values = new_column_values.replace('[',"")
    new_column_values = new_column_values.replace(']',"")
    print("new_column_values")
    print(new_column_values)
    print("get_new_column_values end")
    return new_column_values

from datetime import date, datetime
import csv

def get_yyyy_mm_dd(s):
    yyyy_mm_dd_w_time = str(datetime.strptime(s, '%m/%d/%Y'))
    print("yyyy_mm_dd_w_time")
    print(yyyy_mm_dd_w_time)
    yyyy_mm_dd = yyyy_mm_dd_w_time[:10]
    print("yyyy_mm_dd")
    print(yyyy_mm_dd)
    return yyyy_mm_dd

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

sql = "select [customer],[month end (mm/dd/yyyy)],[license plate],[vin],[vehicle make],[vehicle model],[year],[vehicle type],[dispatcher car id] from [%s].[dbo].[vw_gen_easy_vehicle] order by [customer],[dispatcher car id],[license plate]" % (sql_server_database)
print("sql")
print(sql)
sql_server_cursor.execute(sql)

old_company = "old_company"
new_company = "new_company"

old_month_end = "old_month_end"
new_month_end = "new_month_end"

f_break = False
f_open = False
for row in sql_server_cursor:
    print(row)
    new_company = row[0]
    new_company = new_company.rstrip()
    new_company = new_company.replace(" ","_")
    print("old_company "+old_company)
    print("new_company "+new_company)

    new_month_end = row[1]
    print("old_month_end "+old_month_end)
    new_month_end = get_yyyy_mm_dd(new_month_end)
    print("new_month_end "+new_month_end)

    #sys.exit()
    if old_company != new_company:
        f_break = True
    if old_month_end != new_month_end:
        f_break = True
    if f_break:
        f_break = False
        old_company = new_company
        old_month_end = new_month_end
        fname = root_folder_gen+new_company+'+'+report_name+'+'+new_month_end+'.csv'
        print(fname)
        if not f_open:
            f = open(fname,'w')
            f_open = True
            f.write(new_header)
            f.write('\n')
        else:
            f.close()
            f = open(fname,'w')
            f.write(new_header)
            f.write('\n')
        print("create a new csv file")
    print("write a row")

    row[1] = str(row[1])

    #row[4] = str(row[4])
    #if row[4] == "None":
    #    row[4] = ""

    #row[6] = str(row[6])
    #if row[6] == "None":
    #    row[6] = ""

    #row[7] = str(row[7])
    #if row[7] == "None":
    #    row[7] = ""

    #row[8] = str(row[8])
    #if row[8] == "None":
    #    row[8] = ""

    rowAsList = [x for x in row]
    print(row)
    del rowAsList[0]
    print(rowAsList)
    
    new_column_values = get_new_column_values(rowAsList)
    f.write(new_column_values)
    f.write('\n')

    #sys.exit()
f.close()
sql_server_connection.close()
logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')
