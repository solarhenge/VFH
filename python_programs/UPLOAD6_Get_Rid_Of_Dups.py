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

sql = "update [%s].[dbo].[upload_easy_driver] set [activation_date_of_driver] = null where [activation_date_of_driver] = '1900-01-01 00:00:00.0000000'" % (database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)

sql = "update [%s].[dbo].[upload_easy_driver] set [deactivation_date_of_driver] = null where [deactivation_date_of_driver] = '1900-01-01 00:00:00.0000000'" % (database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)

dml = "select *"
sql = "%s from [%s].[dbo].[upload_easy_driver] where 1 = 1 and [upload_easy_driver_id] in ( select a.[upload_easy_driver_id] from [%s].[dbo].[upload_easy_driver] a join [%s].[dbo].[vw_upload_easy_driver_count_gt_1] b on a.[customer] = b.[customer] and a.[month_end] = b.[month_end] and a.[drivers_license] = b.[drivers_license] and a.[createdon] is null)" % (dml, database_vfh, database_vfh, database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)
dml = "delete"
sql = "%s from [%s].[dbo].[upload_easy_driver] where 1 = 1 and [upload_easy_driver_id] in ( select a.[upload_easy_driver_id] from [%s].[dbo].[upload_easy_driver] a join [%s].[dbo].[vw_upload_easy_driver_count_gt_1] b on a.[customer] = b.[customer] and a.[month_end] = b.[month_end] and a.[drivers_license] = b.[drivers_license] and a.[createdon] is null)" % (dml, database_vfh, database_vfh, database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)

dml = "select *"
sql = "%s from [%s].[dbo].[upload_easy_driver] where 1 = 1 and [upload_easy_driver_id] in ( select a.[upload_easy_driver_id] from [%s].[dbo].[upload_easy_driver] a join [%s].[dbo].[vw_upload_easy_driver_max_createdon] b on a.[customer] = b.[customer] and a.[month_end] = b.[month_end] and a.[drivers_license] = b.[drivers_license] and a.[createdon] <> b.[max_createdon])" % (dml, database_vfh, database_vfh, database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)
dml = "delete"
sql = "%s from [%s].[dbo].[upload_easy_driver] where 1 = 1 and [upload_easy_driver_id] in ( select a.[upload_easy_driver_id] from [%s].[dbo].[upload_easy_driver] a join [%s].[dbo].[vw_upload_easy_driver_max_createdon] b on a.[customer] = b.[customer] and a.[month_end] = b.[month_end] and a.[drivers_license] = b.[drivers_license] and a.[createdon] <> b.[max_createdon])" % (dml, database_vfh, database_vfh, database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)

dml = "select *"
sql = "%s from [%s].[dbo].[upload_easy_vehicle] where 1 = 1 and [upload_easy_vehicle_id] in ( select a.[upload_easy_vehicle_id] from [%s].[dbo].[upload_easy_vehicle] a join [%s].[dbo].[vw_upload_easy_vehicle_count_gt_1] b on a.[customer] = b.[customer] and a.[month_end] = b.[month_end] and a.[plate_license] = b.[plate_license] and a.[createdon] is null)" % (dml, database_vfh, database_vfh, database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)
dml = "delete"
sql = "%s from [%s].[dbo].[upload_easy_vehicle] where 1 = 1 and [upload_easy_vehicle_id] in ( select a.[upload_easy_vehicle_id] from [%s].[dbo].[upload_easy_vehicle] a join [%s].[dbo].[vw_upload_easy_vehicle_count_gt_1] b on a.[customer] = b.[customer] and a.[month_end] = b.[month_end] and a.[plate_license] = b.[plate_license] and a.[createdon] is null)" % (dml, database_vfh, database_vfh, database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)

dml = "select *"
sql = "%s from [%s].[dbo].[upload_easy_vehicle] where 1 = 1 and [upload_easy_vehicle_id] in ( select a.[upload_easy_vehicle_id] from [%s].[dbo].[upload_easy_vehicle] a join [%s].[dbo].[vw_upload_easy_vehicle_max_createdon] b on a.[customer] = b.[customer] and a.[month_end] = b.[month_end] and a.[plate_license] = b.[plate_license] and a.[createdon] <> b.[max_createdon])" % (dml, database_vfh, database_vfh, database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)
dml = "delete"
sql = "%s from [%s].[dbo].[upload_easy_vehicle] where 1 = 1 and [upload_easy_vehicle_id] in ( select a.[upload_easy_vehicle_id] from [%s].[dbo].[upload_easy_vehicle] a join [%s].[dbo].[vw_upload_easy_vehicle_max_createdon] b on a.[customer] = b.[customer] and a.[month_end] = b.[month_end] and a.[plate_license] = b.[plate_license] and a.[createdon] <> b.[max_createdon])" % (dml, database_vfh, database_vfh, database_vfh)
logger1 = logging.getLogger(sys.argv[0])
logger1.info(sql)
#sys.exit()
execute_sql_server(sql)

sql_server_connection.close()            
logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')
