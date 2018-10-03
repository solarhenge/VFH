echo off
IF %1.==. GOTO No1
c:\windows\py.exe UPLOAD1_Move_Copy_CSV_Files.py %1
c:\windows\py.exe UPLOAD3_Rename_CSV_Files.py %1
c:\windows\py.exe UPLOAD4_Process_CSV_Files.py %1
c:\windows\py.exe UPLOAD6_Get_Rid_Of_Dups.py %1
GOTO Success
:No1
  ECHO No param 1 - Environment
:Success
  ECHO Success
