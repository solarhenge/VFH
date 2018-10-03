echo off
IF %1.==. GOTO No1
c:\windows\py.exe GEN_EASY_DRIVER.py %1
c:\windows\py.exe GEN_EASY_TRIP.py %1
c:\windows\py.exe GEN_EASY_VEHICLE.py %1
c:\windows\py.exe GEN_EASY_MOVE_FILES.py %1
GOTO Success
:No1
  ECHO No param 1 - Environment
:Success
  ECHO Success
