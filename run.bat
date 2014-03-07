@echo off
REM	@author Viktor Dmitriyev
echo "Loading data to SAP HANA ..."
c:\Soft\sap\hdbclient\Python\python.exe gdelt_data_loader.py > generated_queries.txt
REM c:\Soft\sap\hdbclient\Python\python.exe -m cProfile gdelt_data_loader.py > generated_queries.txt
pause