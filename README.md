About
=====
Simple python script that loads data from GDELT dataset to SAP HANA DB.

General options
===============
* Load sample data from GDELT event database http://gdeltproject.org/data.html.

Dependencies Setup
==================
* Add path to python that is shipped with "SAP HANA Client" to PATH variable.
* Copy 3 files(__init__.py, dbapi.py, resultthrow.py) from the 'hdbcli' folder to the "SAP HANA Client"'s Python's folder called 'Lib'.
* Copy 2 files(pyhdbcli.pdb, pyhdbcli.pyd) from the 'hdbclient' folder to the "SAP HANA Client"'s Python's folder 'Lib'.
* Additional (http://scn.sap.com/community/developer-center/hana/blog/2012/06/08/sap-hana-and-python-yes-sir)


GDELT Table Structure
====================================
* Table structure is taken from GDELT table definition - http://gdeltproject.org/data/lookups/SQL.tablecreate.txt
* To create table in SAP HANA script 'gdelt_dailyupdates.hdbtable' can be used


Load GDELT Daily Updates Your PC
======================================================
* Move to the subdirectory data
* Run on bat file
	```
		'gdelt_download.bat'
	```
* Or in cmd
	```
	python gdelt_download_daily.py fetch -d . -U
	```

	Specifying Credentials for the SAP HANA DB user
=====================================
* Create file 'sap_hana_credentials.py'
* Copy->Paste code below and insert your credentials
```
# Server 
SERVER = '<server>'
PORT = <port>

# User Credentials
USER = '<user>'
PASSWORD = '<password>'
```

Run on Windows
==============
Run 'run.bat'.
```
run.bat
```	

Known Problems and Drawbacks
============================
* Not all event from daily updates are parsed properly (some shift in data is possible);
* All fields(if generated from SAP HANA .hdbtable) are generated as 'NVARCHAR' data type;

Credits
=======
* Code for downloading zip files with GDELT data was written by John Beieler (johnbeieler.org) and forked from https://github.com/00krishna/gdelt_download (https://github.com/00krishna)