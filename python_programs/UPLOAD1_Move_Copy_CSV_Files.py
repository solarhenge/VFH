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

def get_customer(database, tablename, columnname, s):
    print('database '+database)
    print('tablename '+tablename)
    print('s '+s)
    sql = "select customer from [%s].[dbo].[%s] where %s = '%s'" % (database, tablename, columnname, s)
    print('sql '+sql)
    try:
        execute_sql_server(sql)
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

import glob
import os
import shutil

subfolder_list = ['Original','ETL']

for root, dirs, files in os.walk(root_folder_upload):
    print('root '+root)
    if not any(ext in root for ext in subfolder_list):
        for fname in files:
            print('fname '+fname)

            company_name = get_company_name(fname, database_vfh)
            company_name = company_name.rstrip()
            company_name = company_name.replace(" ","_")
            print('company_name')
            print(company_name)

            company_dir = os.path.join(root, company_name)

            original_dir = os.path.join(company_dir, 'Original')
            etl_dir = os.path.join(company_dir, 'ETL')
            print('company_dir '+company_dir)
            print('original_dir '+original_dir)
            print('etl_dir '+etl_dir)
        
            if not os.path.isdir(company_dir):
                os.mkdir(company_dir)
                os.mkdir(original_dir)
                os.mkdir(etl_dir)

            old_file_path = root
            old_file_path = os.path.join(root, fname)
            print('old_file_path '+old_file_path)
            original_file_path = os.path.join(root, original_dir)
            original_file_path = os.path.join(original_dir, fname)
            print('original_file_path '+original_file_path)
            etl_file_path = os.path.join(root, etl_dir)
            etl_file_path = os.path.join(etl_dir, fname)
            print('etl_file_path '+etl_file_path)

            shutil.move(old_file_path, original_file_path)
            logger1 = logging.getLogger(sys.argv[0]+' '+fname)
            logger1.info(old_file_path+' moved to '+original_file_path)

            shutil.copy(original_file_path, etl_file_path)
            logger1 = logging.getLogger(sys.argv[0]+' '+fname) 
            logger1.info(original_file_path+' copied to '+etl_file_path)

logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')
