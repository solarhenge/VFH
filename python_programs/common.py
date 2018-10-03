import csv
import sys
import time
import os

def check_environment(environment):
    environment_list = ['local','dev','test','prod']
    if not any(ext in environment for ext in environment_list):
        print('environment '+environment+' not found in environment_list '+', '.join(environment_list))
        sys.exit()

def check_report(report):
    report_list = ['driver','trip','vehicle']
    if not any(ext in report for ext in report_list):
        print('report '+report+' not found in report_list '+', '.join(report_list))
        sys.exit()

def get_properties_file_name(environment):
    properties_file_name = 'properties'+'.'+environment
    if not os.path.isfile(properties_file_name):
        print('properties_file_name '+properties_file_name+' not found')
        sys.exit()
    return properties_file_name

def get_property_list(properties_file_name):
    with open(properties_file_name, 'r') as csv_file:
        reader = csv.reader(csv_file)
        property_list = list(reader)
    return property_list

def get_property_value(properties_file_name,property_name):
    property_list = get_property_list(properties_file_name)
    for hay in property_list:
        if property_name in hay:
            if hay[1] == '':
                print(property_name+' property value is null')
                sys.exit()
            return hay[1]
    print('property_name '+property_name+' not found')
    sys.exit()
 
def find_folder_name(folder_name):
    if not os.path.isdir(folder_name):
        print('folder_name '+folder_name+' not found')
        sys.exit()

def get_logging_filename(python_filename,log_folder):
    timestr = time.strftime("%Y%m%d_%H%M%S")
    scriptname = python_filename
    logname = scriptname.replace('.py','')
    filename = logname+'+'+timestr+'.log'
    logging_filename = log_folder+filename
    return logging_filename
