from datetime import datetime
from io import StringIO
import os

import civis
import pandas as pd

from components import SFTP_Components_Extract as Extract
from components import SFTP_Components_Load as Load
from components.email import send_email as email



def main(SFTP_Conn, S3):
    srv = SFTP_Conn
    s3 = S3

    
    files = srv.listdir()

    #print("Contents of folder: {}".format(files))
    
    for dats in files:
        #if srv.isfile(dats):
        if '.AMT' in dats:
            with srv.open(dats, 'r') as city_hall_table:
                new_dat = city_hall_table.read()
                ## Add actual header names to this from the existing SQL table
                new_dat = b'[comma,delim,headers,string]\r\n' + new_dat
            dat_encode = str(new_dat,'utf-8')
        
            try:
                data = StringIO(dat_encode)
            except Exception as err:
                er_msg = "Could not StingIO the encoded data string!\nError: {}".format(err)
                print(er_msg)
            
                # Email part
                try:
                    em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                 + er_msg)
                    email.send_email(email_subject="SFTP Transfer Error",
                                     email_body=em_msg,
                                     service_key=os.environ['EMAIL_PASSWORD'])
                    
                except Exception as err:
                    print("Email failed\nError: {}".format(err))
                
            else:
                print("Data encoded")
        
            try:
                # Have to move the buffer back to 0 in order for it to spit out the needed data
                data.seek(0)
                # Read it in as a table 
                df = pd.read_table(data, sep=',')
            except Exception as err:
                er_msg = "Could not convert data to dataframe!\nError: {}".format(err)
                print(er_msg)
            
                # Email part
                try:
                    em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                 + er_msg)
                    email.send_email(email_subject="SFTP Transfer Error",
                                     email_body=em_msg,
                                     service_key=os.environ['EMAIL_PASSWORD'])
                except Exception as err:
                    print("Email failed\nError: {}".format(err))
                
            else:
                print("Dataframe created")
        
            try:
                df['extracted'] = datetime.now()
            except Exception as err:
                er_msg = "Could not add 'extracted' column to dataframe!\nError: {}".format(err)
                print(er_msg)
            
                # Email part
                try:
                    em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                 + er_msg)
                    email.send_email(email_subject="SFTP Transfer Error",
                                     email_body=em_msg,
                                     service_key=os.environ['EMAIL_PASSWORD'])
                except Exception as err:
                    print("Email failed\nError: {}".format(err))
                    
            else:
                print("Extracted column added")
        
            try:
                civ_up = civis.io.dataframe_to_civis(df=df,
                                                     database="[DATABASENAME]",
                                                     table="[SCHEMA.TABLE]",
                                                     existing_table_rows='append')
            except Exception as err:
                er_msg = "Could not upload {} to civis!\nError: {}".format(dats,err)
                print(er_msg)
            
                # Email part
                try:
                    em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                 + er_msg)
                    email.send_email(email_subject="SFTP Transfer Error",
                                     email_body=em_msg,
                                     service_key=os.environ['EMAIL_PASSWORD'])
                    
                except Exception as err:
                    print("Email failed\nError: {}".format(err))
            else:
                try:
                    civ_up.result()
                except Exception as err:
                    er_msg = "Couldn't get .result() from civis upload...\nError: {}".format(err)
                    print(er_msg)
            
                    # Email part
                    try:
                        em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                     + er_msg)
                        email.send_email(email_subject="SFTP Transfer Error",
                                         email_body=em_msg,
                                         service_key=os.environ['EMAIL_PASSWORD'])
                        
                    except Exception as err:
                        print("Email failed\nError: {}".format(err))
            
                print("Uploaded to civis")
        
        
            try:
                Load.SFTP_S3_Transfer(SFTP_Conn = srv,
                                      S3_Conn = s3,
                                      dest_bucket = '[dest/bucket]',
                                      files = [dats])
                
            except Exception as err2:
                er_msg = "SFTP to S3 Transfer failed!\nError: {}".format(err2)
                print(er_msg)
            
                # Email part
                try:
                    em_msg = str("There was an error while trying to perform the SFTP transfer\n\n"
                                 + er_msg)
                    email.send_email(email_subject="SFTP Transfer Error", 
                                     email_body=em_msg,
                                     service_key=os.environ['EMAIL_PASSWORD'])
                    
                except Exception as err:
                    print("Email failed\nError: {}".format(err))
            else:
                print("Backed up to S3")
                # Delete file from SFTP
                srv.remove(dats)
        

if __name__ == '__main__':
    
   
    srv = Extract.SFTP_Connect(IP_add='[IP_ADDRESS]',
                               uname=os.environ['SFTP_USER'],
                               pwrd=os.environ['SFTP_PWD'],
                               conn_dir="[directory/path/to/files]",
                               show_contents = False)
    
    s3 = Extract.S3_backup_connect(aws_etl_key=os.environ['AWS_ETL_ACCESS_KEY_ID'],
                                   aws_secret_key=os.environ['AWS_ETL_SECRET_ACCESS_KEY'],
                                   bucket='[bucket-name]')
    

    main(SFTP_Conn=srv, S3=s3)
    
print("Done!")
