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

#SQL Server begin
sql_server_connect_string = common.get_property_value(properties_file_name,'sql_server_connect_string')
print('sql_server_connect_string')
print(sql_server_connect_string)

database_flx = common.get_property_value(properties_file_name,'database_flx')
print('database_flx')
print(database_flx)

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

#Oracle begin
oracle_server_name = common.get_property_value(properties_file_name,'oracle_server_name')
print('oracle_server_name')
print(oracle_server_name)

oracle_port = common.get_property_value(properties_file_name,'oracle_port')
print('oracle_port')
print(oracle_port)

oracle_services = common.get_property_value(properties_file_name,'oracle_services')
print('oracle_services')
print(oracle_services)

oracle_user = common.get_property_value(properties_file_name,'oracle_user')
#print('oracle_user')
#print(oracle_user)

oracle_password = common.get_property_value(properties_file_name,'oracle_password')
#print('oracle_password')
#print(oracle_password)

import cx_Oracle
try:
    dsn_tns = cx_Oracle.makedsn(oracle_server_name, oracle_port, oracle_services)
    oracle_connection = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn_tns)
except cx_Oracle.Error as ex:
    #sqlstate = ex.args[0]
    print(ex)
    logger2 = logging.getLogger(dsn_tns)
    logger2.error(ex)
    sys.exit()
oracle_cursor = oracle_connection.cursor()
#Oracle end

tablename = 'DISPATCHER'

sql = "DELETE FROM [%s].[dbo].[%s] WHERE 1 = 1 AND CREATEDON < GETDATE()-7" % (database_flx, tablename)
print("sql")
print(sql)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
execute_sql_server(sql)

old_header = ['DISPATCHER_TYPE','PERMIT_UID' ,'PERMIT_NUMBER' ,'CONTROL_GROUP' ,'STATUS' ,'CUSTOMER_UID' ,'CUSTOMER' ,'GROUP_OPERATE_AS' ,'EMAIL_ADDRESS' ,'CUSTOMER_START_DATE' ,'CUSTOMER_END_DATE' ,'PERMIT_EFFECTIVE_DATE' ,'PERMIT_EXPIRATION_DATE' ,'CREATEDON'] 
print('old_header')
print(old_header)
import json
new_header = json.dumps(old_header)
new_header = new_header.replace('[',"")
new_header = new_header.replace(']',"")
new_header = new_header.replace('"',"")
print('new_header')
print(new_header)

from datetime import date, datetime

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

sql = "select dispatcher_type,permit_uid,permit_number,control_group,status,customer_uid,customer,group_operate_as,email_address,customer_start_date,customer_end_date,permit_effective_date,permit_expiration_date,sysdate AS createdon from vfh_dispatcher_view"
print("sql")
print(sql)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
oracle_cursor.execute(sql)

for row in oracle_cursor:
    print(row)
    GROUP_OPERATE_AS = 'NULL'
    CUSTOMER_START_DATE = 'NULL'
    CUSTOMER_END_DATE = 'NULL'
    PERMIT_EFFECTIVE_DATE = 'NULL'
    PERMIT_EXPIRATION_DATE = 'NULL'
    if row[7] is not None:
        GROUP_OPERATE_AS = row[7]
    if row[9] is not None:
        CUSTOMER_START_DATE = json_serial(row[9])
    if row[10] is not None:
        CUSTOMER_END_DATE = json_serial(row[10])
    if row[11] is not None:
        PERMIT_EFFECTIVE_DATE = json_serial(row[11])
    if row[12] is not None:
        PERMIT_EXPIRATION_DATE = json_serial(row[12])
    if row[13] is not None:
        CREATEDON = json_serial(row[13])
    print("GROUP_OPERATE_AS "+GROUP_OPERATE_AS)
    print("CUSTOMER_START_DATE "+CUSTOMER_START_DATE)
    print("CUSTOMER_END_DATE "+CUSTOMER_END_DATE)
    print("PERMIT_EFFECTIVE_DATE "+PERMIT_EFFECTIVE_DATE)
    print("PERMIT_EXPIRATION_DATE "+PERMIT_EXPIRATION_DATE)
    print("CREATEDON "+CREATEDON)
    old_values = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],GROUP_OPERATE_AS,row[8],CUSTOMER_START_DATE,CUSTOMER_END_DATE,PERMIT_EFFECTIVE_DATE,PERMIT_EXPIRATION_DATE,CREATEDON]
    print('old_values')
    print(old_values)
    new_values = json.dumps(old_values)
    new_values = new_values.replace("'","''")
    new_values = new_values.replace('[',"")
    new_values = new_values.replace(']',"")
    new_values = new_values.replace('"',"'")
    new_values = new_values.replace("'NULL'","NULL")
    print('new_values')
    print(new_values)

    sql = "INSERT INTO [%s].[dbo].[%s] ( %s ) VALUES ( %s )" % (database_flx, tablename, new_header, new_values)
    print("sql")
    print(sql)
    logger1 = logging.getLogger(sys.argv[0])
    logger1.info(sql)
    execute_sql_server(sql)
sql_server_connection.close()
oracle_connection.close()
logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')
