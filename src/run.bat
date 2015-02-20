@echo off
REM	@author Viktor Dmitriyev
echo "Loading GDELT data into SAP HANA DB ..."
REM c:\Soft\sap\hdbclient\Python\python.exe gdelt_data_loader.py
c:\Soft\sap\rev80\hdbclient\Python\python.exe gdelt_data_loader.py > generated_output.txt
REM c:\Soft\sap\hdbclient\Python\python.exe -m cProfile gdelt_data_loader.py > generated_queries.txt
pause