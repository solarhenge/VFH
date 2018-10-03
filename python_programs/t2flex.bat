echo off
IF %1.==. GOTO No1
c:\windows\py.exe T2FLEX1_DISPATCHER.py %1
c:\windows\py.exe T2FLEX2_FEE_YYYYMM.py %1
c:\windows\py.exe T2FLEX3_CUSTOMER.py %1
GOTO Success
:No1
  ECHO No param 1 - Environment
:Success
  ECHO Success
