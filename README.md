# Project: Vehicle For Hire (VFH)

## Purpose
The purpose of this project is to manage VFH data that is submitted on a monthly basis via the internet. VFH are taxis, limousines and other vehicles, including those hired by way of an online application.

## Project Description
In this project, I have written a number of python programs that facilitate an ETL pipeline. The output is a series of csv files that can be used by financial resources for invoicing purposes.

## How To Execute


echo off
IF %1.==. GOTO No1
c:\windows\py.exe copy_upload_files.py %1
c:\windows\py.exe move_upload_files.py %1
call t2flex.bat %1
call upload.bat %1
call success_upload.bat %1
call etl_file_name.bat %1
call gen_easy.bat %1
GOTO Success
:No1
  ECHO No param 1 - Environment
:Success
  ECHO Success
  
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
