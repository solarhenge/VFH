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
#SQL Server end

import time
current_yyyymm = time.strftime("%Y%m")
print("current_yyyymm "+current_yyyymm)

import datetime
today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
previous_yyyymm = lastMonth.strftime("%Y%m")
print("previous_yyyymm "+previous_yyyymm)

def find_yyyymm(database, tablename, yyyymm):
    print('database '+database)
    print('tablename '+tablename)
    print('yyyymm '+yyyymm)
    countstar = 0
    sql = "select count(*) from [%s].[dbo].[%s] where yyyymm = %s" % (database, tablename, yyyymm)
    print('sql '+sql)
    sql_server_cursor.execute(sql)
    result=sql_server_cursor.fetchone()
    number_of_rows=result[0]
    print(number_of_rows)
    return number_of_rows
    #sys.exit()

def fee_yyyymm_gst(database, tablename, previous_yyyymm, current_yyyymm):
    print('database '+database)
    print('tablename '+tablename)
    print('previous_yyyymm '+previous_yyyymm)
    print('current_yyyymm '+current_yyyymm)
    number_of_rows = find_yyyymm(database, tablename, previous_yyyymm)
    if number_of_rows == 0:
        sys.exit()
    number_of_rows = find_yyyymm(database, tablename, current_yyyymm)
    if number_of_rows == 0:
        print('do stuff')
        sql = "insert into [%s].[dbo].[%s] ([yyyymm] , [gst] ) select %s,[gst] from [%s].[dbo].[%s] where yyyymm = %s" % (database, tablename, current_yyyymm, database, tablename, previous_yyyymm)
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
        sql_server_connection.commit()

def fee_grp(database, tablename, previous_yyyymm, current_yyyymm):
    print('database '+database)
    print('tablename '+tablename)
    print('previous_yyyymm '+previous_yyyymm)
    print('current_yyyymm '+current_yyyymm)
    number_of_rows = find_yyyymm(database, tablename, previous_yyyymm)
    if number_of_rows == 0:
        sys.exit()
    number_of_rows = find_yyyymm(database, tablename, current_yyyymm)
    if number_of_rows == 0:
        sql = "insert into [%s].[dbo].[%s] ([yyyymm],[fee_group_type_id],[fee_type_id],[fee_unit_frequency_type_id],[notes],[fee],[display_order]) select %s,[fee_group_type_id],[fee_type_id],[fee_unit_frequency_type_id],[notes],[fee],[display_order] from [%s].[dbo].[%s] where yyyymm = %s" % (database, tablename, current_yyyymm, database, tablename, previous_yyyymm)
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
        sql_server_connection.commit()

tablename = 'fee_yyyymm_gst'
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' begin processing...')
fee_yyyymm_gst(database_vfh, tablename, previous_yyyymm, current_yyyymm)
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' end processing...')

tablename = 'fee_grp_licence_driver'
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' begin processing...')
fee_grp(database_vfh, tablename, previous_yyyymm, current_yyyymm)
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' end processing...')

tablename = 'fee_grp_licence_vehicle'
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' begin processing...')
fee_grp(database_vfh, tablename, previous_yyyymm, current_yyyymm)
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' end processing...')

tablename = 'fee_grp_other'
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' begin processing...')
fee_grp(database_vfh, tablename, previous_yyyymm, current_yyyymm)
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' end processing...')

tablename = 'fee_grp_ptp'
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' begin processing...')
fee_grp(database_vfh, tablename, previous_yyyymm, current_yyyymm)
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' end processing...')

tablename = 'fee_grp_taxi'
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' begin processing...')
fee_grp(database_vfh, tablename, previous_yyyymm, current_yyyymm)
logger1 = logging.getLogger(sys.argv[0]+' '+tablename)
logger1.info(tablename+' end processing...')
logger1 = logging.getLogger(sys.argv[0])
logger1.info('end processing...')
