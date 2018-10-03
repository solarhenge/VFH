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

from collections import OrderedDict
import csv
import datetime as dt
import glob
import os
import time

extension = '.csv'
report_name_list = ['EASY_TRIP','EASY_DRIVER','EASY_VEHICLE']

import json

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def get_UploadedOn(fname):
    print('get_UploadedOn begin')
    print('fname '+fname)
    plus2_hyphen = find_between(fname, "+2", "-")
    #print("plus2_hyphen "+plus2_hyphen)
    UploadedOn = '2'+plus2_hyphen
    #print("UploadedOn "+UploadedOn)
    UploadedOn = dt.datetime.strptime(UploadedOn, "%Y%m%d_%H%M%S")
    UploadedOn = str(UploadedOn)
    print("UploadedOn "+UploadedOn)
    print('get_UploadedOn end')
    return UploadedOn

def get_current_datetime():
    print('get_current_datetime begin')
    now_date = dt.datetime.now().date()
    print(now_date)
    now_time = dt.datetime.now().time()
    print(now_time)
    current_datetime = str(now_date)+" "+str(now_time)
    print(current_datetime)
    print('get_current_datetime end')
    return current_datetime

def get_customer(database, tablename, columnname, s):
    print('database '+database)
    print('tablename '+tablename)
    print('s '+s)
    sql = "select customer from [%s].[dbo].[%s] where %s = '%s'" % (database, tablename, columnname, s)
    print('sql '+sql)
    try:
        sql_server_cursor.execute(sql)
        logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
        logger1.info(sql)
    except pyodbc.DataError as e:
        data_error = True
        logger2 = logging.getLogger(sys.argv[0]+' '+tablename)
        logger2.error(e)
    except pyodbc.ProgrammingError as e:
        data_error = True
        logger2 = logging.getLogger(sys.argv[0]+' '+tablename)
        logger2.error(e)
    result=sql_server_cursor.fetchone()
    return result

def get_company_name(fname, database):
    company_name = fname.split("+",1)[0]
    print('company_name')
    print(company_name)
    if company_name is None:
        logger1 = logging.getLogger(sys.argv[0]+' '+fname) 
        logger1.info('company_name not found in fname')
        sys.exit()
    company_name = company_name.replace("_"," ")
    company_name = company_name.replace("'","''")
    print('company_name')
    print(company_name)
    customer = get_customer(database, 'vw_customer_group_operate_as', 'group_operate_as', company_name)
    print('sql_server_cursor.rowcount')
    print(str(sql_server_cursor.rowcount))
    if sql_server_cursor.rowcount == 0:
        customer = get_customer(database, 'vw_customer_group_operate_as', 'customer', company_name)
        print('sql_server_cursor.rowcount')
        print(str(sql_server_cursor.rowcount))
    if sql_server_cursor.rowcount == 0:
        logger1 = logging.getLogger(sys.argv[0]+' '+fname) 
        logger1.info('customer not found in vw_customer_group_operate_as')
        sys.exit()
    print('customer')
    print(customer[0])
    return customer[0]

def get_report_name(s):
    report_name_list = ['EASY_TRIP','EASY_DRIVER','EASY_VEHICLE']
    for x in report_name_list:
        if x in s:
            return x
    return None

def transform_column_names(column_names):
    print("transform_column_names begin")
    print("column_names")
    print(column_names)
    column_names = [x.strip(' ') for x in column_names]
    new_column_names = json.dumps(column_names)
    new_column_names = new_column_names.replace("'","")
    new_column_names = new_column_names.replace('"',"")
    new_column_names = new_column_names.replace('[',"")
    new_column_names = new_column_names.replace(']',"")
    new_column_names = new_column_names.replace(', ',',')
    print("new_column_names")
    print(new_column_names)
    print("transform_column_names end")
    return new_column_names

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
    return yyyy_mm_dd

def transform_column_values(column_names, column_values):
    print("transform_column_values begin")
    print("column_names")
    print(column_names)
    print("column_values")
    print(column_values)

    try:
        index_date1 = column_names.index("month end (mm/dd/yyyy)")
        print("index_date1")
        print(str(index_date1))
        if index_date1 > -1:
            print(column_values[index_date1])
            if column_values[index_date1] != "":
                column_values[index_date1] = convert_cowboy_dates(column_values[index_date1]) 
    except ValueError as e:
        print(e)

    try:    
        index_date2 = column_names.index("activation date of driver (mm/dd/yyyy)")
        print("index_date2")
        print(str(index_date2))
        if index_date2 > -1:
            print(column_values[index_date2])
            if column_values[index_date2] != "":
                column_values[index_date2] = convert_cowboy_dates(column_values[index_date2]) 
    except ValueError as e:
        print(e)

    try:
        index_date3 = column_names.index("deactivation date of driver (mm/dd/yyyy)")
        print("index_date3")
        print(str(index_date3))
        if index_date3 > -1:
            print(column_values[index_date3])
            if column_values[index_date3] != "":
                column_values[index_date3] = convert_cowboy_dates(column_values[index_date3]) 
    except ValueError as e:
        print(e)

    column_values = [x.strip(' ') for x in column_values]
    new_column_values = json.dumps(column_values)
    new_column_values = new_column_values.replace('"',"'")
    new_column_values = new_column_values.replace('[',"")
    new_column_values = new_column_values.replace(']',"")
    print("new_column_values")
    print(new_column_values)
    print("transform_column_values end")
    return new_column_values

for root, dirs, files in os.walk(root_folder_upload):
    if 'ETL' in root:
        print('root '+root)
        for fname in files:
            print("fname "+fname)
            if os.path.splitext(fname)[-1] == extension:
                if any(ext in fname for ext in report_name_list):
                    report_name = get_report_name(fname)
                    print("report_name "+report_name)
                    print('fname does contain a report_name')
                    company_name_dir = root 
                    print("company_name_dir "+company_name_dir)
                    company_name_dir_fname = os.path.join(root, fname) 
                    print("company_name_dir_fname "+company_name_dir_fname)
                    new_fname = company_name_dir_fname
                    new_fname = new_fname+'.csv.*'

                    if glob.glob(new_fname):
                        print(company_name_dir_fname+' has already been processed')
                    else:

                        company_name = get_company_name(fname, database_vfh)
                        company_name = company_name.replace("'","''")
                        #sys.exit()

                        UploadedOn = get_UploadedOn(fname)

                        logger1 = logging.getLogger(sys.argv[0]+' '+fname)
                        logger1.info(company_name_dir_fname+' begin processing...')

                        data_error = False

                        #with open(company_name_dir_fname, encoding='utf-8') as f:
                        with open(company_name_dir_fname) as f:

                            for row in csv.DictReader(f):

                                #print("row ")
                                #print(row)
                                myDict = OrderedDict(row)

                                columns = ", ".join(myDict.keys())
                                print("columns")
                                print(columns)

                                old_column_names = list(myDict.keys())
                                old_column_names.append('UploadedOn')
                                old_column_names.append('CreatedOn')
                                print('old_column_names')
                                print(old_column_names)

                                new_column_names = transform_column_names(old_column_names)
                                new_column_names = new_column_names.replace(' (mm/dd/yyyy)','')
                                new_column_names = new_column_names.replace(' ','_')

                                new_column_names = new_column_names.replace('license_plate','plate_license')
                                new_column_names = new_column_names.replace('year','year_of_manuf')
                                
                                print('new_column_names')
                                print(new_column_names)
    
                                old_column_values = list(myDict.values())
                                old_column_values.append(UploadedOn)

                                CreatedOn = get_current_datetime()
                                print("CreatedOn")
                                print(CreatedOn)
                                old_column_values.append(CreatedOn)

                                print('old_column_values')
                                print(old_column_values)
                                
                                len_old_column_names = len(old_column_names)
                                print("len_old_column_names")
                                print(len_old_column_names)

                                new_column_values = transform_column_values(old_column_names, old_column_values)
                                print('new_column_values')
                                print(new_column_values)

                                sql = "INSERT INTO [%s].[dbo].[UPLOAD_%s] ( %s, %s ) VALUES ( '%s', %s )" % (database_vfh, report_name, 'CUSTOMER', new_column_names, company_name, new_column_values)
                                print("sql")
                                print(sql)
                                #sys.exit()

                                logger1 = logging.getLogger(sys.argv[0]+' '+fname)
                                logger1.info(company_name_dir_fname+' '+sql)
                       
                                try:
                                    sql_server_cursor.execute(sql)
                                except pyodbc.DataError as e:
                                    data_error = True
                                    #sqlstate = e.args[0]
                                    print(company_name_dir_fname)
                                    print(e)
                                    logger2 = logging.getLogger(sys.argv[0]+' '+fname)
                                    logger2.error(e)
                                    #sys.exit()
                                except pyodbc.ProgrammingError as e:
                                    data_error = True
                                    #sqlstate = e.args[0]
                                    print(company_name_dir_fname)
                                    print(e)
                                    logger2 = logging.getLogger(sys.argv[0]+' '+fname)
                                    logger2.error(e)
                                    #sys.exit()
                                sql_server_connection.commit()

                        print("company_name_dir_fname "+company_name_dir_fname)
                        suffix = 'SUFFIX'
                        if data_error:
                            suffix = 'FAILURE'
                        else:
                            suffix = 'SUCCESS'
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

sql_server_connection.close()               
logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')
