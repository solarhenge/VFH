echo off
IF %1.==. GOTO No1
c:\windows\py.exe ETL_FILE_NAME.py %1
c:\windows\py.exe ETL_FILE_NAME_MOVE_FILES.py %1
GOTO Success
:No1
  ECHO No param 1 - Environment
:Success
  ECHO Success
