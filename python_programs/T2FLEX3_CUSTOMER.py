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

import json
def get_new_values(old_values):
    print(old_values)
    new_values = json.dumps(old_values)
    new_values = new_values.replace("'","''")
    new_values = new_values.replace('[',"")
    new_values = new_values.replace(']',"")
    new_values = new_values.replace('"',"'")
    new_values = new_values.replace("'NULL'","NULL")
    print('new_values')
    print(new_values)
    return new_values

tablename = 'CUSTOMER'

sql = "DELETE FROM [%s].[dbo].[%s] WHERE 1 = 1 AND CREATEDON < GETDATE()-7" % (database_flx, tablename)
print("sql")
print(sql)
execute_sql_server(sql)

old_header = ['CUSTOMER_UID' ,'CURRENT_CUSTOMER' ,'CURRENT_GROUP_OPERATE_AS' ,'PREVIOUS_CUSTOMER' ,'PREVIOUS_GROUP_OPERATE_AS'] 
print('old_header')
print(old_header)
new_header = ['CUSTOMER_UID' ,'CURRENT_CUSTOMER' ,'CURRENT_GROUP_OPERATE_AS' ,'PREVIOUS_CUSTOMER' ,'PREVIOUS_GROUP_OPERATE_AS' ,'CREATEDON'] 
print('new_header')
print(new_header)
import json
new_header = json.dumps(new_header)
new_header = new_header.replace('[',"")
new_header = new_header.replace(']',"")
new_header = new_header.replace('"',"")
print('new_header')
print(new_header)

sql = "SELECT DISTINCT t.[customer_uid] , t.[customer] [current_customer] , t.[group_operate_as] [current_group_operate_as] , tprev.[customer] [previous_customer] , tprev.[group_operate_as] [previous_group_operate_as] FROM [%s].[DBO].[dispatcher] AS t INNER JOIN [%s].[DBO].[dispatcher] AS tprev ON tprev.[customer_uid] = t.[customer_uid] AND tprev.[createdon] < t.[createdon] WHERE 1  = 1 ORDER BY t.[customer_uid]" % (database_flx, database_flx)
print("sql")
print(sql)
sql_server_cursor.execute(sql)
result = sql_server_cursor.fetchall()
for row in result:
    print(row[1])
    print(row[2])
    print(row[3])
    print(row[4])

    OLD_GROUP_OPERATE_AS = 'NULL'
    NEW_GROUP_OPERATE_AS = 'NULL'
    if row[2] is not None:
        NEW_GROUP_OPERATE_AS = row[2]
    if row[4] is not None:
        OLD_GROUP_OPERATE_AS = row[4]

    old_values = [row[0],row[1],NEW_GROUP_OPERATE_AS,row[3],OLD_GROUP_OPERATE_AS]

    new_values = [row[0],row[1],NEW_GROUP_OPERATE_AS,row[3],OLD_GROUP_OPERATE_AS]
    CreatedOn = get_current_datetime()
    print("CreatedOn")
    print(CreatedOn)
    new_values.append(CreatedOn)
    new_values = get_new_values(new_values)
    do_insert = False
    if row[1] != row[3]:
        do_insert = True
        print('change in customer')
    if row[2] != row[4]:
        do_insert = True
        print('change in group_operate_as')
    if do_insert:
        sql = "INSERT INTO [%s].[dbo].[%s] ( %s ) VALUES ( %s )" % (database_flx, tablename, new_header, new_values)
        print("sql")
        print(sql)
        execute_sql_server(sql)

sql_server_connection.close()
logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')

