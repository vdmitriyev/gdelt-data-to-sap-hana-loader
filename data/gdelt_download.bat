@echo off
REM	@author Viktor Dmitriyev
REM python gdelt_download_daily.py fetch -d . -U
REM python gdelt_download_daily.py fetch_start_date -d . -U -S 20140201 -E 20140228
python gdelt_download_daily.py fetch_start_date -d . -U -S 20140301
pause