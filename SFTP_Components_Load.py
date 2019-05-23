import json
import re
import os
from datetime import datetime
import boto
import civis
import pandas as pd
from pathlib import PurePath
import pysftp

from components.transform.python3.preprocess import get_mod_date


# Load
def SFTP_S3_Transfer(SFTP_Conn, S3_Conn, dest_bucket, files = None):
    # Pull in the list of sftp names
    if files:
        try:
            files_to_move = files
        except Exception as err:
            print('Error Establishing Connection: {}'.format(err))

            # set up the srv.get line
        for file in files_to_move:
            if SFTP_Conn.isdir(file):
                continue
            try:
                SFTP_Conn.isfile(file)
            except Exception as err:
                print("Could not find {} in current SFTP directory.\nError: {}".format(file,err))
            else:
                print('Starting file '
                      + str(files_to_move.index(file)+1)
                      + ' of '
                      + str(len(files_to_move))
                      + ' at ' + str(datetime.now())
                     )
                # Check the size to determine pull type
                d = SFTP_Conn.listdir_attr(file)
                if d[0].st_size > 30000:
                    # pull the file with open/write
                    try:
                        with SFTP_Conn.open(file) as fl_read:
                            dat_trans = fl_read.read()
                        with open(file, 'wb') as dest_file:
                            dest_file.write(dat_trans)            
                    except Exception as err:
                        print('Could not get file from SFTP: {}'.format(err))
                        break
                else:
                    # pull the file with .get()        
                    try:
                        SFTP_Conn.get(file, preserve_mtime=True)
                    except Exception as err:
                        print('Could not get file from SFTP: {}'.format(err))
                        break
                    # hand it off to S3
                try:
                    file_trans = S3_Conn.new_key(
                        str(PurePath('sftp-backups', dest_bucket, file)))
                    file_trans.set_contents_from_filename(
                        str(PurePath(file)), encrypt_key='KMS')
                except Exception as err:
                    print('Could not transfer ' + str(file) +
                          '\nError: {}'.format(err) + '\nMoving on...')
                    continue
    else:
        try:
            files_to_move = SFTP_Conn.listdir()
        except Exception as err:
            print('Error Establishing Connection: {}'.format(err))

        # set up the srv.get line
        for file in files_to_move:
            if SFTP_Conn.isdir(file):
                continue
            print('Starting file '
                  + str(files_to_move.index(file)+1)
                  + ' of '
                  + str(len(files_to_move))
                  + ' at ' + str(datetime.now())
                  )
            # Check the size to determine pull type
            d = SFTP_Conn.listdir_attr(file)
            if d[0].st_size > 30000:
                # pull the file with open/write
                try:
                    with SFTP_Conn.open(file) as fl_read:
                        dat_trans = fl_read.read()
                    with open(file, 'wb') as dest_file:
                        dest_file.write(dat_trans)            
                except Exception as err:
                    print('Could not get file from SFTP: {}'.format(err))
                    break
            else:
            # pull the file with .get()        
                try:
                    SFTP_Conn.get(file, preserve_mtime=True)
                except Exception as err:
                    print('Could not get file from SFTP: {}'.format(err))
                    break
            # hand it off to S3
            try:
                file_trans = S3_Conn.new_key(
                    str(PurePath('sftp-backups', dest_bucket, file)))
                file_trans.set_contents_from_filename(
                    str(PurePath(file)), encrypt_key='KMS')
            except Exception as err:
                print('Could not transfer ' + str(file) +
                      '\nError: {}'.format(err) + '\nMoving on...')
                continue


def Local_S3_Upload(file_dir_path, file_name, S3_Conn, dest_bucket_dir):
    # Pull in the list of sftp names
    print('Starting upload at ' + str(datetime.now()))
    # hand it off to S3
    try:
        file_trans = S3_Conn.new_key(str(PurePath(dest_bucket_dir, file_name)))
        file_trans.set_contents_from_filename(
            str(PurePath(file_dir_path, file_name)), encrypt_key="KMS")
    except Exception as err:
        print('Could not transfer ' + str(file_name) +
              '\nError: {}'.format(err) + '\nMoving on...')


def SFTP_Delete(SFTP_Conn, test_run=True):
    try:
        files_to_kill = SFTP_Conn.listdir()
    except Exception as err:
        print('Bad SFTP connection: {}'.format(err))

    if test_run:
        print("TEST RUN! No deleting will actually happen until test_run variable is set to 'False'")
        print('You will be deleting the following files:')
        for file in files_to_kill:
            print(file)
    else:
        for file in files_to_kill:
            try:
                SFTP_Conn.remove(file)
            except Exception as err:
                print(str(file) + ': Could not be deleted !!!')
                print('Error: {}'.format(err))
                print('Skipping...')
                continue
            print(str(file) + ': Deleted at ' + str(datetime.now()))


def SFTP_to_Civis_Add(SFTP_Conn, schema, table, if_exists, delim, keyword=None):
    # Connect to civis api to check table's most recent modified date
    client = civis.APIClient()
    DELIMITERS = {'comma': ',',
                  'tab': '\t',
                  'pipe': '|'
                  }
    try:
        civis_tbl_id = client.get_table_id(str(schema + '.' + table), 'Boston')
    except Exception as err:
        print("Error: {}".format(err))

    tbl_mod_dt = get_mod_date(civis_tbl_id)

    try:
        files_to_move = SFTP_Conn.listdir()
    except Exception as err:
        print('Could not establish connection with S3: {}'.format(err))

    # set up the srv.get line
    if keyword is None:
        for file in files_to_move:
            if SFTP_Conn.isdir(file):
                continue
            print('Starting file ' + str(file) + ' at ' + str(datetime.now()))
            d = SFTP_Conn.listdir_attr(file)
            if d[0].st_size > 30000:
            # pull the file with open/write
                try:
                    with SFTP_Conn.open(file) as fl_read:
                        dat_trans = fl_read.read()
                    with open(i, 'wb') as dest_file:
                        dest_file.write(dat_trans)
                    # Get original 'last modified' datetime    
                    file_date_time = datetime.utcfromtimestamp(
                        d[0].st_mtime).strftime('%Y-%m-%d')
                    
                except Exception as err:
                    print('Could not get file from SFTP: {}'.format(err))
                    break
                
            else:
                # pull the file
                try:
                    SFTP_Conn.get(file, preserve_mtime=True)
                except Exception as err:
                    print('Could not connect to SFTP Server: {}'.format(err))
                    break
                # get the date it was created
                file_date_time = int(os.path.getmtime(file))
                file_date_time = datetime.utcfromtimestamp(
                    file_date_time).strftime('%Y-%m-%d')

            if tbl_mod_dt <= file_date_time:
                try:
                    pandas_data_export = pd.read_table(
                        str(PurePath(file)), sep=DELIMITERS.get(delim))
                except Exception as err:
                    print("Couldn't load dataframe, error: {}".format(err))
                try:
                    cup = civis.io.dataframe_to_civis(df=pandas_data_export,
                                                      database='Boston',
                                                      table=str(
                                                          schema + '.' + table),
                                                      existing_table_rows=if_exists
                                                      )
                    print(cup.result())
                    print('Uploaded' + ' at ' + str(datetime.now()))
                except Exception as err:
                    print('Could not upload table to civis. Error: {}'.format(err))
                    continue
    else:
        for file in files_to_move:
            if SFTP_Conn.isdir(file):
                continue
            # pull the file
            if keyword in file:
                print('Starting file ' + str(file) 
                      + ' at ' + str(datetime.now()))
                d = SFTP_Conn.listdir_attr(file)
                if d[0].st_size > 30000:
                    # pull the file with open/write
                    try:
                        with SFTP_Conn.open(file) as fl_read:
                            dat_trans = fl_read.read()
                        with open(i, 'wb') as dest_file:
                            dest_file.write(dat_trans)
                        # Get original 'last modified' datetime    
                        file_date_time = datetime.utcfromtimestamp(
                            d[0].st_mtime).strftime('%Y-%m-%d')
                    
                    except Exception as err:
                        print('Could not get file from SFTP: {}'.format(err))
                        break
                
                else:
                    # pull the file
                    try:
                        SFTP_Conn.get(file, preserve_mtime=True)
                    except Exception as err:
                        print('Could not connect to SFTP Server: {}'.format(err))
                        break
                # get the date it was created
                    file_date_time = int(os.path.getmtime(file))
                    file_date_time = datetime.utcfromtimestamp(
                        file_date_time).strftime('%Y-%m-%d')

                if tbl_mod_dt <= file_date_time:
                    try:
                        pandas_data_export = pd.read_table(
                            str(PurePath(file)), sep=DELIMITERS.get(delim))
                    except Exception as err:
                        print("Couldn't load dataframe, error: {}".format(err))
                    try:
                        cup = civis.io.dataframe_to_civis(df=pandas_data_export,
                                                          database='Boston',
                                                          table=str(
                                                              schema + '.' + table),
                                                          existing_table_rows=if_exists
                                                          )
                        print(cup.result())
                        print('Uploaded' + ' at ' + str(datetime.now()))
                    except Exception as err:
                        print('Could not upload table to civis. Error: {}'.format(err))
                        continue
