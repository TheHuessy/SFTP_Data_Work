import json
import os
import re

import boto
import civis
from datetime import datetime
import pandas as pd
from pathlib import PurePath
import pysftp
import zipfile

from components.extract.python3 import SFTP_Transfer as Extract
from components.load.python3 import SFTP_Transfer as Load
from components.email import send_email as email

table_keys = {"ActivityEnrollment": "bcyf_internal_data.activity_enrollment",
              "Manifest": "bcyf_internal_data.manifest",
              "MemberData": "bcyf_internal_data.member_data",
              "Attendance": "bcyf_internal_data.attendance",
              "MembershipEnrollment": "bcyf_internal_data.attendance"}

def Recent_File(SFTP_Conn):
    gf = None
    t = datetime.utcfromtimestamp(1366666666).strftime('%Y-%m-%d %H:%M:%S')
    dirs = SFTP_Conn.listdir()
    g = [i for i in dirs]
    for i in g:
        if ".zip" not in i:
            continue
        h = SFTP_Conn.listdir_attr(i)
        t2 = h[0].st_mtime
        t2 = datetime.utcfromtimestamp(t2).strftime('%Y-%m-%d %H:%M:%S')
        if t2 > t:
            t = t2
            gf = i
    return(gf)
    if gf is None:
        print("No file matched needed criteria to get time information. Check connection directory")
        return(None)
    


def main(SFTP_Conn, S3_Conn):
    del_list = []
    
    try:
        get_file = Recent_File(SFTP_Conn = SFTP_Conn)
    except Exception as err:
        print("Could not get recent file\nError: {}".format(err))        
    else:
        print("File chosen to unzip: {}".format(get_file))
        try:
            with SFTP_Conn.open(get_file) as e:                
                uz = zipfile.ZipFile(e, 'r')                
                uz.extractall("app/{}".format("uz"))                
                uz.close
        except Exception as err:
            er_msg = "Was not able to unzip file from SFTP to container!\nError: {}".format(err)
            print(er_msg)
            
            # Email part
            em_msg = str("There was an error while trying to perform the BCYF SFTP transfer\n\n"
                         + er_msg)
            email.send_email(email_subject="SFTP Transfer Error", email_body=em_msg, service_key=os.environ['EMAIL_SERVICE_USERNAME'])
 
    ## Backup to S3 ##
    
        
    print("Backing {} up to S3 at {}".format(get_file, datetime.now()))
    try:
        Load.SFTP_S3_Transfer(SFTP_Conn, S3_Conn, dest_bucket = 'bcyf/nfocus/trax7', files = [get_file])
    except Exception as err:
        er_msg = "SFTP to S3 Transfer failed!\nError: {}".format(err)
        
        print(er_msg)
            
        # Email part
        em_msg = str("There was an error while trying to perform the BCYF SFTP transfer\n\n"
                         + er_msg)
        email.send_email(email_subject="SFTP Transfer Error", email_body=em_msg, service_key=os.environ['EMAIL_SERVICE_USERNAME'])
    else:
        del_list += [get_file]
        
    print("Back up Complete at {}".format(datetime.now()))
        
    ## Load to Civis ##
            
    uz_contents = os.listdir("{}".format("app/uz"))
    for file in uz_contents:
        try:
            dat = pd.read_csv("app/{}/{}".format("uz",file))        
        except Exception as err:
            er_msg = "Was not able to load extracted file {} to df.\nError: {}".format(file,err)
            print(er_msg)
            
            # Email part
            em_msg = str("There was an error while trying to perform the BCYF SFTP transfer\n\n"
                         + er_msg)
            email.send_email(email_subject="SFTP Transfer Error", email_body=em_msg, service_key=os.environ['EMAIL_SERVICE_USERNAME'])
                         
            continue
            
        short_name = re.sub(pattern='.csv', repl="", string=str(file))
        
        try:
            civt = civis.io.dataframe_to_civis(df=dat, database="Boston", table=table_keys[short_name], existing_table_rows='drop')
        except Exception as err:
            er_msg = "Was not able to load file {} to Civis.\nError: {}".format(file,err)
            print(er_msg)
            
            # Email part
            em_msg = str("There was an error while trying to perform the BCYF SFTP transfer\n\n"
                         + er_msg)
            email.send_email(email_subject="SFTP Transfer Error", email_body=em_msg, service_key=os.environ['EMAIL_SERVICE_USERNAME'])
        else:
            civt.result()
            
#    if del_list:        
#        for kill_file in del_list:
#            print("Attempting removal of {} from the SFTP Server...".format(kill_file))
#            try:
#                SFTP_Conn.remove(kill_file)
#            except OSError:
#                SFTP_Conn = Extract.SFTP_Connect(IP_add='140.241.251.81', uname=os.environ['SFTP_USER'], pwrd=os.environ['SFTP_PWD'], conn_dir='BCYF/nFocus/Trax7/Backup', show_contents = False)
#                try:
#                    SFTP_Conn.remove(kill_file)
#                except Exception as err:
#                    er_msg = "It looks like {} refuses to die!\nError: {}".format(kill_file,err)
#                    print(er_msg)
#            
#                    # Email part
#                    em_msg = str("There was an error while trying to perform the BCYF SFTP transfer\n\n"
#                                 + er_msg)
#                    email.send_email(email_subject="SFTP Transfer Error", email_body=em_msg, service_key=os.environ['EMAIL_SERVICE_USERNAME'])
#                    continue
#                else:
#                    print("{} has been removed from the SFTP Server".format(kill_file))
#            else:
#                print("{} has been removed from the SFTP Server".format(kill_file))
#    else:
#        print("No del_list found, no files to delete")
    
if __name__ == '__main__':
            
    print("Connecting to SFTP Server at {}".format(datetime.now()))
    srv = Extract.SFTP_Connect(IP_add='140.241.251.81', uname=os.environ['SFTP_USER'], pwrd=os.environ['SFTP_PWD'], conn_dir='SFTP_Root/BCYF/nFocus/Trax7', show_contents = False)

    print("Connecting to S3 at {}".format(datetime.now()))
    s3 = Extract.S3_backup_connect(aws_etl_key=os.environ['AWS_ETL_KEY'], aws_secret_key=os.environ['AWS_ETL_SECRET'], bucket='city-of-boston')

    main(SFTP_Conn=srv, S3_Conn=s3)
    
print("Finished Transfer")
