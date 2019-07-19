# Project: Vehicle For Hire (VFH)

## Purpose
The purpose of this project is to manage VFH data that is submitted on a monthly basis via the internet. VFH are taxis, limousines and other vehicles, including those hired by way of an online application.

## Project Description
In this project, I have written a number of python programs that facilitate an ETL pipeline. The output is a series of csv files that can be used by financial resources for invoicing purposes.

## How To Execute
In the "Project Workspace" "Launcher" tab click on the "Terminal" square.
At the command prompt type the following in order:

bigkahuna.bat

### bigkahuna.bat
will execute the following:
* copy_upload_files.py
* move_upload_files.py
* t2flex.bat
* upload.bat
* success_upload.bat
* etl_file_name.bat
* gen_easy.bat

### t2flex.bat
will execute the following:
* t2flex1_dispatcher.py
* t2flex2_fee_yyyymm.py
* t2flex3_customer.py

### upload.bat
will execute the following:
* upload1_move_copy_csv_files.py
* upload3_rename_csv_files.py
* upload4_process_csv_files.py
* upload6_get_rid_of_dups.py

### success_upload.bat
will execute the following:
* success_upload_driver.py
* success_upload_trip.py
* success_upload_trip_transpose.py
* success_upload_trip_duplicate.py
* success_upload_vehicle.py
* success_upload_move_files.py

### etl_file_name.bat
will execute the following:
* etl_file_name.py
* etl_file_name_move_files.py

### gen_easy.bat
will execute the following:
* gen_easy_driver.py
* gen_easy_trip.py
* gen_easy_vehicle.py
* gen_easy_move_files.py




