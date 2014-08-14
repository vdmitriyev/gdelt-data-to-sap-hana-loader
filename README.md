About
=====
Simple python script that loads data from GDELT dataset to SAP HANA DB table.

Dependencies Setup
==================
* Add path to python that is shipped with "SAP HANA Client" to PATH variable.
* Copy 3 files(__init__.py, dbapi.py, resultthrow.py) from the 'hdbcli' folder to the "SAP HANA Client"'s Python's folder called 'Lib'.
* Copy 2 files(pyhdbcli.pdb, pyhdbcli.pyd) from the 'hdbclient' folder to the "SAP HANA Client"'s Python's folder 'Lib'.
* Additional (http://scn.sap.com/community/developer-center/hana/blog/2012/06/08/sap-hana-and-python-yes-sir)


GDELT Table Structure
====================================
* Table structure is taken from GDELT table definition - http://gdeltproject.org/data/lookups/SQL.tablecreate.txt
* To create table in SAP HANA script use following script 'gdelt_dailyupdates.hdbtable'
* Directory "data" contains python script that fetches daily data updates(interval can be specified) from GDELT website and stores and upzips them on your PC.


Load GDELT Daily Updates from PC
======================================================
* Move to the subdirectory data
* Run on bat file
```
'gdelt_download.bat'
```
* in command line (for only daily updates)
```
python gdelt_download_daily.py fetch_from_date -d "../zipped" -U -du "../unzipped"
```
* in command line (fromdate &lt;option -F and date in format 'YYYYMMDD'&gt;, todate &lt;option -T and date in format 'YYYYMMDD'&gt;)
```
python gdelt_download_daily.py fetch_from_date -d "../zipped" -U -du "../unzipped" -F 20140321
```


Credentials for the SAP HANA DB
======================================
* Create file 'sap_hana_credentials.py'
* Copy-&gt;Paste code below and insert your credentials
```
# Server 
SERVER = '<server>'
PORT = <port>

# User Credentials
USER = '<user>'
PASSWORD = '<password>'
```

Applications &lt;port&gt; should be 3&lt;instance number&gt;15. 
For example, 30015, if the instance is 00.

Run on Windows
==============
To main python script on windows machine you can use 'run.bat'.
Note: (a) all configurations must be performed before script can be executed properly;
```
run.bat
```	

Known Problems and Drawbacks
============================
* [FIXED] Not all event from daily updates are parsed properly (some shift in data is possible);
* [FIXED] All fields(if generated from SAP HANA .hdbtable) are generated as 'NVARCHAR' data type;

Credits
=======
* Code for downloading zip files with GDELT data was written by John Beieler (johnbeieler.org) and forked from https://github.com/00krishna/gdelt_download (https://github.com/00krishna)