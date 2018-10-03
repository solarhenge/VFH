echo off
IF %1.==. GOTO No1
c:\windows\py.exe SUCCESS_UPLOAD_DRIVER.py %1
c:\windows\py.exe SUCCESS_UPLOAD_TRIP.py %1
c:\windows\py.exe SUCCESS_UPLOAD_TRIP_TRANSPOSE.py %1
c:\windows\py.exe SUCCESS_UPLOAD_TRIP_DUPLICATE.py %1
c:\windows\py.exe SUCCESS_UPLOAD_VEHICLE.py %1
c:\windows\py.exe SUCCESS_UPLOAD_MOVE_FILES.py %1 driver
c:\windows\py.exe SUCCESS_UPLOAD_MOVE_FILES.py %1 trip
c:\windows\py.exe SUCCESS_UPLOAD_MOVE_FILES.py %1 vehicle
GOTO Success
:No1
  ECHO No param 1 - Environment
:Success
  ECHO Success
