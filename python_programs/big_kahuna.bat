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
