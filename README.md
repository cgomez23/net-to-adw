# Email Data Scrape to OAC Workflow
Workflow:
Part 1: Gmail → Oracle VM
Part 2: Oracle VM → ADW

Description: This script will run on a virtual machine in our Oracle Cloud infrastructure, downloading each csv for each table and uploading it to the requested table in ADW. The source code is currently in a Dev-Ops instance (dev-ops-code) within a git repository in Oracle. A multithreading design was implemented into this code for faster performance. 

# Part 1: Download

There are two requirements needed for the script to work, the table name and the label/subject of the email. The label and subject of the email are configured to be the same, so the script uses one criterion for two. The script searches for the specific email for the upload and downloads the attachment to the virtual machine.

# Part 2: Upload

After downloading the csv into a designated folder in the virtual machine, the script then prepares a custom SQL query to upload the specific data to the requested table. The data is also formatted to meet SQL standards for uploading. After this is finished, the script then executes the SQL query on a batch of data, either inserting or updating rows in the table.
