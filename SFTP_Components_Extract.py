import json
import re
import os
from datetime import datetime
import boto
import civis
import pysftp



def S3_backup_connect(aws_etl_key, aws_secret_key, bucket = [BUCKETNAME]):
    #set up connection to S3
    try:
        conn = boto.connect_s3(aws_access_key_id = aws_etl_key, aws_secret_access_key = aws_secret_key)
        buk = conn.get_bucket(bucket)
        print('Connected to bucket {}!'.format(bucket))
        return(buk)
        
    except Exception as err:
        print('Could not connect to S3: {}'.format(err))
              


def SFTP_Connect(IP_add, uname, pwrd, conn_dir, show_contents=False):
    conn_opts = pysftp.CnOpts()
    conn_opts.hostkeys = None
    try:
        server = pysftp.Connection(host = IP_add, username = uname, password = pwrd, cnopts = conn_opts)
    except Exception as err:
        print("A Connection error occured: {}".format(err))
    
    if conn_dir:
        try:
            server.chdir('/E:/' + conn_dir + '/')
        except Exception as err:
            print("A directory error occured: {}".format(err))
    else:
        try:
            server.chdir('/E:/')
        except Exception as err:
            print("A directory error occured: {}".format(err))
            
    print('Connected to SFTP Server!')
    if show_contents:
        files = server.listdir()
        print('Directory Contents:')
        for i in files:
            print(i)            
return(server) 
