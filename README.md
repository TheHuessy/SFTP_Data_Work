# SFTP Related Data Work

These scripts are examples of various methods I implement when writing ETL pipelines between an SFTP server and a cloud data warehouse. All scripts are intended to be run in a headless environment or docker container.

## [SFTP_Components_Extract.py](#SFTP_Components_Extractpy):
A few components written to be used across multiple scripts that perform various extraction related tasks. These functions create connections to an SFTP server using the pysftp module as well as a connection to an S3 account using the boto package. They are set up to be modular, allowing devs to connect to any server or S3 account easily.

## [SFTP_Components_Load.py](#SFTP_Components_Loadpy):
More commonly used functions to be called from multiple scripts that perform load related tasks. These work with either the SFTP connection made from SFTP_Connect() or the S3 connection from S3_Connect() in [SFTP_Components_Extract.py](#SFTP_Components_Extractpy). These functions include one to back up a given file or directoy contents to S3, a function to upload a file locally to S3 (for when a file needs to be manipulated locally before being backed up), a function to a delete given file or directory contents from an SFTP server, and a function to load a data file's contents from an SFTP server directly to a cloud data warehouse.

## [SFTP_Data_Without_Headers.py](#SFTP_Data_Without_Headerspy):
This script performs an ETL job between the SFTP server, backing up the data files to S3, and loading the data to the cloud. The data in this pipeline are uploaded without headers and so there is logic baked in to add headers and correct encoding before passing it to the cloud.

## [SFTP_Multiple_Locations.py](#SFTP_Multiple_Locationspy):
This script performs an ETL job that has to pull data from sub directories and loads different files to different locations in the cloud based on a master dictionary.

## [SFTP_Unzip_Backup_Example.py](#SFTP_Unzip_Backup_Examplepy):
This script performs a more resource intensive ETL job that unzips a large zip file on the SFTP server, extracts the contents, loads them to the cloud, and backs up the zip file to S3. This specific job required a change in the initial SFTP transfer logic. It turns out that both the paramiko and pysftp modules have a file size limit when trying to call the `.get()` function when pulling files. If one tries, it will hang and eventually break. I added logic that pulls the file size and uses the appropriate method of extraction from the SFTP server to keep this bug from happening.




