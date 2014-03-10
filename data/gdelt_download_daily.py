from __future__ import print_function
from __future__ import unicode_literals
import os
import io
import sys
import time
import zipfile
import requests
import argparse
import datetime
import os.path

__author__ = 'John Beieler, johnbeieler.org'
__email__ = 'jub270@psu.edu'


from datetime import date, timedelta

def perdelta(start, end, delta):
  """
    (date, date, date) -> date

    Recieves "start" and "end" and returns dates between them with inteval specified in "delta".
  """

  curr = start
  while curr <= end:
    yield curr
    curr += delta

def convert_date(date_to_convert):
  """
    (str) -> (int, int, int)

    Recieves paramters "date_to_convert" in format 'yyyyddmm' (for example '20130101') and converts it to the int.
  """

  return int(date_to_convert[:4]), int(date_to_convert[4:6]), int(date_to_convert[6:])

def get_gdelt_daily_updates_from_date(directory, from_date, to_date=None):
  """
    (str, date) -> NoneType

    Getting GDELT daily data from given date to the current date.

  """

  if to_date is None:
    to_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

  from_year, from_month, from_day = convert_date(from_date)
  to_year, to_month, to_day = convert_date(to_date) 

  print ("%s %s %s " % (from_year, from_month, from_day))
  print ("%s %s %s " % (to_year, to_month, to_day))

  for temp_date in perdelta(date(from_year, from_month, from_day), date(to_year, to_month, to_day), timedelta(days=1)):
    
    url = '%s.export.CSV.zip' % (temp_date.strftime("%Y%m%d"))
    file_name = '%s.export.CSV' % (temp_date.strftime("%Y%m%d"))

    get_url = 'http://data.gdeltproject.org/events/{}'.format(url)
    print('Downloading {}'.format(url))

    if not os.path.isfile( directory + '/' + file_name):
    	written_file = _download_chunks(directory, get_url)
    	_unzip_file(directory, written_file)

def get_daily_data(directory, unzip=False):
    """
    Function to download the daily update file from the GDELT website.

    Parameters
    ----------

    directory: String.
               Directory to write the downloaded file.

    unzip: Boolean.
           Argument that indicates whether or not to unzip the downloaded
           file. Defaults to False.

    """
    now = datetime.date.today()-datetime.timedelta(days=1)
    year = now.year
    month = now.month
    day = now.day
    url = '%04d%02d%02d.export.CSV.zip' % (year, month, day)
    get_url = 'http://data.gdeltproject.org/events/{}'.format(url)
    print('Downloading {}'.format(url))
    written_file = _download_chunks(directory, get_url)
	
    if unzip:
        _unzip_file(directory, written_file)


def get_upload_daily_data(directory, bucket, folder_name):
    """
    Function to download the daily update file from the GDELT website.

    Parameters
    ----------

    directory: String.
               Directory to write the downloaded file.


    bucket: String.
            Name of the bucket on Amazon.

    folder_name: String.
                 Name of the folder within the bucket to which you want to
                 upload the file. This should be something like 'daily/' which
                 will create the key under 's3_bucket/daily/filename'. It is
                 important to note that Amazon S3 doesn't actually have a
                 concept of folders. This string is really just an extension
                 to the key.

    """
    now = datetime.date.today()-datetime.timedelta(days=1)
    year = now.year
    month = now.month
    day = now.day
    url = '%04d%02d%02d.export.CSV.zip' % (year, month, day)
    get_url = 'http://data.gdeltproject.org/events/{}'.format(url)
    print('Downloading {}'.format(url))
    written_file = _download_chunks(directory, get_url)
    final_file = _unzip_file(directory, written_file)
    upload_filename = url.replace('.zip', '')
    s3_upload(final_file, upload_filename, bucket, folder_name)


def s3_upload(unzipped_file, filename, s3_bucket, folder=None):
    """"
    Function to upload downloaded GDELT daily files to Amazon S3. Requires
    the boto library for Python.

    Parameters
    ----------

    unzipped_file: String.
                   Filepath to the downloaded and unzipped daily update file.

    filename: String.
              Name of the file. This is used as the key to upload the file to
              Amazon.

    s3_bucket: String.
               Name of the bucket on Amazon.

    folder: String.
            Name of the folder within the bucket to which you want to upload
            the file. This should be something like 'daily/' which will create
            the key under 's3_bucket/daily/filename'. It is important to note
            that Amazon S3 doesn't actually have a concept of folders. This
            string is really just an extension to the key.

    """
    import boto
    from boto.s3.key import Key
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(s3_bucket)
    k = Key(bucket)
    if folder:
        print('Uploading to {}{}'.format(folder, filename))
        k.key = '{}{}'.format(folder, filename)
    else:
        print('Uploading to {}'.format(filename))
        k.key = filename
    k.set_contents_from_filename(unzipped_file)
    print('Upload complete!')


def _unzip_file(directory, zipped_file):
    """
    Private function to unzip a zipped file that was downloaded.

    Parameters
    ----------

    directory: String.
               Directory to write the unzipped file.

    zipped_file: String.
                 Filepath of the zipped file to unzip.

    """
    print('Unzipping {}'.format(zipped_file))
    try:
        z = zipfile.ZipFile(zipped_file)
        for name in z.namelist():
            f = z.open(name)
            out_path = os.path.join(directory, name)
            with io.open(out_path, 'w', encoding='utf-8') as out_file:
                content = f.read().decode('utf-8')
                out_file.write(content)
        print('Done unzipping {}'.format(zipped_file))
        return out_path
    except zipfile.BadZipfile:
        print('Bad zip file for {}, passing.'.format(zipped_file))


def _download_chunks(directory, url):
    """
    Private function to download a zipped file in chunks.

    Parameters
    ----------

    directory: String.
               Directory to write the downloaded file.

    url: String.
         URL of the file to download.

    """

    base_file = os.path.basename(url)

    temp_path = directory
	
    try:
        local_file = os.path.join(temp_path, base_file)

        req = requests.get(url, stream=True)
        with io.open(local_file, 'wb') as fp:
            for chunk in req.iter_content(chunk_size=1024):
                if chunk:
                    fp.write(chunk)
    except requests.exceptions.HTTPError as e:
        print("HTTP Error: {}; {}".format(e, url))
    # except requests.exceptions.URLError as e:
    #    print("URL Error: {}; {}".format(e, url))

    return local_file

if __name__ == '__main__':

    print('Running...')
    aparse = argparse.ArgumentParser()

    sub_parse = aparse.add_subparsers(dest='command_name')

    fetch_command = sub_parse.add_parser('fetch', help="""Download only the
                                         daily update for today's date.""",
                                         description="""Download only the
                                         daily update for today's date.""")

    fetch_command.add_argument('-d', '--directory', help="""Path of directory
                               for file download""")

    fetch_command.add_argument('-U', '--unzip', action='store_true',
                               default=False, help="""Boolean flag indicating
                               whether or not to unzip the downloaded
                               files.""")

    fetch_from_date_command = sub_parse.add_parser('fetch_from_date', 
                                        help="""Download only the daily update starting from .""",
                                        description="""Download updates for begin date.""")
    
    fetch_from_date_command.add_argument('-d', '--directory', 
                                        help="""Path of directory for file download""")

    fetch_from_date_command.add_argument('-U', '--unzip', 
                                        action='store_true',
                                        default=False, 
                                        help="""Boolean flag indicating whether or not to unzip the downloaded files.""")

    fetch_from_date_command.add_argument('-F', '--fromdate',
                                        default=False, 
                                        help="""Specify 'fromdate' to start from.""")

    fetch_from_date_command.add_argument('-T', '--todate',
                                        default=False, 
                                        help="""Specify 'to date', if not specified 'current date' is taken as 'end date'.""")

    fetch_upload_command = sub_parse.add_parser('fetch_upload', help="""Set the
                                                script to run on a daily basis
                                                and upload the results to
                                                Amazon S3. Unzips the files by
                                                default.""",
                                                description="""Set the
                                                script to run on a daily basis
                                                and upload the results to
                                                Amazon S3. Unzips the files by
                                                default.""")
    fetch_upload_command.add_argument('-d', '--directory', help="""Path of
                                      directory for file download""")
    fetch_upload_command.add_argument('--bucket', help="""Amazon S3 bucket
                                      to which files should be uploaded.
                                      Required.""", required=True)
    fetch_upload_command.add_argument('--folder', help="""Optional Argument
                                      indicating a sub-folder within the bucket
                                      to which to add files.""")

    schedule_command = sub_parse.add_parser('schedule', help="""Set the script
                                            to run on a daily basis.""",
                                            description="""Set the script
                                            to run on a daily basis.""")
    schedule_command.add_argument('-d', '--directory', help="""Path of
                                  directory for file download""")
    schedule_command.add_argument('-U', '--unzip', action='store_true',
                                  default=False, help="""Boolean flag
                                  indicating whether or not to unzip the
                                  downloaded files.""")

    upload_command = sub_parse.add_parser('schedule_upload', help="""Set the
                                          script to run on a daily basis and
                                          upload the results to Amazon S3.
                                          Unzips the files by default.""",
                                          description="""Set the
                                          script to run on a daily basis and
                                          upload the results to Amazon S3.
                                          Unzips the files by default.""")
    upload_command.add_argument('-d', '--directory', help="""Path of directory
                                  for file download""")
    upload_command.add_argument('--bucket', help="""Amazon S3 bucket
                                to which files should be uploaded.
                                Required.""",
                                required=True)
    upload_command.add_argument('--folder', help="""Optional Argument
                                  indicating a sub-folder within the bucket
                                  to which to add files.""")

    args = aparse.parse_args()
    directory = args.directory

    if args.command_name == 'fetch':
        get_daily_data(directory, args.unzip)
		
    elif args.command_name == 'fetch_from_date':

      if args.fromdate == False:
      	get_daily_data(directory, args.unzip)
      elif args.todate == False:
        get_gdelt_daily_updates_from_date(directory, args.fromdate)
      else:
        get_gdelt_daily_updates_from_date(directory, args.fromdate, args.todate)

        
    elif args.command_name == 'fetch_upload':
        get_upload_daily_data(directory, args.bucket, args.folder)
    elif args.command_name == 'schedule_upload':
        if sys.version_info[0] == 2:
            import schedule
            schedule.every().day.at("10:00").do(get_upload_daily_data,
                                                directory, args.bucket,
                                                args.folder)
            while 1:
                schedule.run_pending()
                time.sleep(1)
        else:
            print('Only Python 2.x is supported for this command.')
    elif args.command_name == 'schedule':
        import schedule
        schedule.every().day.at("10:00").do(get_daily_data,
                                            directory, args.unzip)
        while 1:
            schedule.run_pending()
            time.sleep(1)
