@echo off
REM	@author Viktor Dmitriyev
REM python gdelt_download_daily.py fetch -d . -U
python gdelt_download_daily.py fetch_from_date -d . -U -F 20140201 -T 20140228
REM python gdelt_download_daily.py fetch_from_date -d . -U -F 20140305
pause