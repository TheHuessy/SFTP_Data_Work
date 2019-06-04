import os

import boto
import civis
from datetime import datetime
import pandas as pd
from pathlib import PurePath
import pysftp
import re

from components import SFTP_Components_Extract as Extract
from components import SFTP_Components_Load as Load
from components.email import send_email as email

sub_folder = {"[Subdirectory_Name1]": ["[Dataset_Name1]", "[schema.table1]", "[S3/destination/bucket1]"],
              "[Subdirectory_Name2]": ["[Dataset_Name2]", "[schema.table2]", "[S3/destination/bucket2]"],
              "[Subdirectory_Name3]": ["[Dataset_Name3]", "[schema.table3]", "[S3/destination/bucket3]"],
              "[Subdirectory_Name4]": ["[Dataset_Name4]", "[schema.table4]", "[S3/destination/bucket4]"],
              "[Subdirectory_Name5]": ["[Dataset_Name5]", "[schema.table5]", "[S3/destination/bucket5]"]
             }

def main(SFTP_Conn, S3):
    
    srv = SFTP_Conn
    s3 = S3
    start_append = False
    
    for pwr in list(sub_folder):
        try:
            srv.chdir(pwr)
        except Exception as err:
            print("It would appear that SSIS already pulled this file before I could get to it.\nError: {}".format(err))
            continue
        
        dest_tbl = re.sub(pattern=".*\.", repl="", string=sub_folder[pwr][1])
        dats = srv.listdir()

        print("Starting {}".format(sub_folder[pwr][0]))

        for csv in dats:
            if srv.isdir(csv):
                continue
            if '.txt' in csv:
                continue
            if '.csv' in csv:
                try:
                    d = srv.listdir_attr(csv)
                except Exception as err:
                    er_msg = "Could not get attributes from filename.\nError: {}".format(err)
                    print(er_msg)
            
                    # Email part
                    em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                 + er_msg)
                    email.send_email(email_subject="SFTP Transfer Error",
                                     email_body=em_msg,
                                     service_key=os.environ['EMAIL_SERVICE_PASSWORD'])
                    
                    
                else:
                    if d[0].st_size > 30000:
                        try:
                            with srv.open(csv) as ddf:
                                dat_trans = ddf.read()
                            with open(data, 'wb') as dest_file:
                                dest_file.write(dat_trans)
                        except Exception as err:
                            er_msg = "Was not able to pull file {} (Large file method). Error: {}".format(csv,err)
                            print(er_msg)
            
                            # Email part
                            em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                         + er_msg)
                            email.send_email(email_subject="SFTP Transfer Error",
                                             email_body=em_msg,
                                             service_key=os.environ['EMAIL_SERVICE_PASSWORD'])
                            
                            continue
                    else:
                        try:
                            srv.get(csv)
                        except Exception as err:
                            er_msg = "Was not able to pull file {} (.get() method). Error: {}".format(csv,err)
                            print(er_msg)
            
                            # Email part
                            em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                         + er_msg)
                            email.send_email(email_subject="SFTP Transfer Error",
                                             email_body=em_msg,
                                             service_key=os.environ['EMAIL_SERVICE_PASSWORD'])
                            
                            continue
                try:
                    datr = pd.read_csv(csv)
                    
                    data = pd.read_csv(csv)
                    data['Building'] = sub_folder[pwr][0]
                except Exception as err:
                    er_msg = "Could not load csv data to pandas.\nError: {}".format(err)
                    print(er_msg)
            
                    # Email part
                    em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                 + er_msg)
                    email.send_email(email_subject="SFTP Transfer Error",
                                     email_body=em_msg,
                                     service_key=os.environ['EMAIL_SERVICE_PASSWORD'])
            
                try:
                    if not start_append:
                        data_all = data
                        start_append = True
                    else:
                        data_all = data_all.append(data)
                except Exception as err:
                    print("Could not load the all data!\nError: {}".format(err))
            
                try:
                    civ_up = civis.io.dataframe_to_civis(df=datr,
                                                         database='[DATABASE_NAME]',
                                                         table=sub_folder[pwr][1],
                                                         existing_table_rows='append')
                except Exception as err:
                    er_msg = "Couldn't load file to {} in civis!\nError: {}".format(dest_tbl,err)
                    
                    print(er_msg)
            
                    # Email part
                    em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                 + er_msg)
                    email.send_email(email_subject="SFTP Transfer Error",
                                     email_body=em_msg,
                                     service_key=os.environ['EMAIL_SERVICE_PASSWORD'])
                
                try:
                    Load.SFTP_S3_Transfer(SFTP_Conn = srv,
                                          S3_Conn = s3,
                                          dest_bucket = sub_folder[pwr][2],
                                          files = [csv])
                except Exception as err:
                    er_msg = "SFTP to S3 Transfer failed!\nError: {}".format(err)
                    print(er_msg)
            
                    # Email part
                    em_msg = str("There was an error while trying to perform the CPower SFTP transfer\n\n"
                                 + er_msg)
                    email.send_email(email_subject="SFTP Transfer Error",
                                     email_body=em_msg,
                                     service_key=os.environ['EMAIL_SERVICE_PASSWORD'])
                else:
                print("Loaded {} to S3".format(csv))
                print("Removing File...")
                try:
                    srv.remove(csv)
                except Exception as err:
                    print("Could not kill {}...\nError: {}".format(csv,err))
                else:
                    print("{} has been removed!".format(csv))
          
            print("Finished with {}".format(sub_folder[pwr][0]))
        
            srv.chdir("..")
    try:
        civ_up = civis.io.dataframe_to_civis(df=data_all,
                                             database='[DATABASE_NAME]',
                                             table='[schema.combined_table]',
                                             existing_table_rows='append')
        
    except Exception as err:
        er_msg = "Couldn't load file to combined table in civis!\nError: {}".format(err)
        print(er_msg)

        # Email part
        
        em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                     + er_msg)
        email.send_email(email_subject="SFTP Transfer Error",
                         email_body=em_msg,
                         service_key=os.environ['EMAIL_SERVICE_PASSWORD'])
    
    print("Doneski")
    
if __name__ == '__main__':    
    
    srv = Extract.SFTP_Connect(IP_add='[IP_ADDRESS]',
                               uname=os.environ['SFTP_USER'],
                               pwrd=os.environ['SFTP_PWD'],
                               conn_dir='[path/to/data/directory]',
                               show_contents = False)
    
    s3 = Extract.S3_backup_connect(aws_etl_key=os.environ['AWS_ETL_ACCESS_KEY_ID'],
                                   aws_secret_key=os.environ['AWS_ETL_SECRET_ACCESS_KEY'],
                                   bucket='[Destination/bucket]')

main(SFTP_Conn = srv, S3 = s3)
