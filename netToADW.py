# Collect the emails

# Build-ins
# 1. Any directory variables specific to the computer, paste in first cell #GOOD
# 2. If the email truncates the results, forward email to Joe and Carlos #GOOD
# 3. If there is an error in the script at all, send error email to Joe and Carlos #GOOD
# 4. Send daily email with last modified date with any main tables (or tables it updates)
#    - Additionally, for invc_cm_v12, customer_union_v12, item_union_v2 tables #GOOD
# 5. Stretch Goal: create master function in where we input table and folder name and run the update query for this specific table
#    - The merge query is finished, work on implementing a large function #GOOD

import json
# Part 1
import email, getpass, imaplib, os,datetime
from email.header import make_header,decode_header
from datetime import datetime as dt, date
import traceback
import smtplib
import sys
import time
import threading
# Part 2
import cx_Oracle as cx
import csv
import os
import warnings
warnings.filterwarnings('ignore')

# Part 1:
# gmail extraction

data = open('config.json', 'r')
config = json.load(data)

# login info
user = config['user']
pwd = config['pass']


def log(table_n, msg):
    f = open(table_n+'_log.txt', 'a')
    if "===" not in msg:
        f.write(dt.now().strftime("%d/%m/%Y %H:%M:%S")+'\t'+msg+table_n+'\n')
    else:
        f.write(msg)

# function to send email if error occurs
# params: exception object and str of custom message; returns void
def sendErrorEmail(ex, custom_msg, table_name, label):
    global user
    global pwd
    trace_str = ''.join(traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)) + "\n"
    message = 'Subject: {}\n\nTable: {}\nLabel: {}\n\n{}'.format(custom_msg, table_name, label, trace_str)

    # list of email_id to send the mail
    li = ["emails to", "send updates to"] #add emails to for regular/error messages
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login(user, pwd)
    for dest in li:
        s.sendmail(user, dest, message)
    
    log(table_name, trace_str)
    s.quit()
    log(table_name, 'Error, check email')
    print(trace_str, '')
    print('Error, check email', '')
    sys.exit()

# function to sent email
# params: str of subject and str of message; returns void
def send_email(subject, msg):
    message = 'Subject: {}\n\n{}'.format(subject, msg)
    
    li = ["emails to", "send updates to"] #add emails to for regular/error messages
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login(user, pwd)
    for dest in li:
        s.sendmail(user, dest, message)
    
    s.quit()

# function return correct data format for input date
def get_format(date):
    for fmt in ('%m/%d/%Y', '%m/%d/%Y %H:%M', '%m/%d/%Y %I:%S %p'):
        try:
            dt.strptime(date, fmt)
            return fmt
        except ValueError:
            pass
    raise ValueError('no valid date format found')

# function to format date and id data to sql standards
def format_data(d, id_idx, date_idxs):
    for i, l in enumerate(d):
        for j, x in enumerate(l):
            if not x:
                d[i][j] = ''
                continue
            if j in date_idxs:
                fmt = get_format(x.upper())
                d[i][j] = dt.strptime(x, fmt).strftime('%d-%b-%y').upper()
                #log(dt.strptime(x, fmt).strftime('%d-%b-%y').upper())
        l.insert(0, l.pop(id_idx)) # IMPORTANT: this is a special case just for when the primary key (internal_id) is not the first column in the table.
                                   # this line switches the internal_id column to the first column.
                                   # cx_oracle.execute() recognizes where a primary key is.
    return d

# this is the master function of the program; it will download the attachment,
# delete the email in Gmail, and upload the attachment (csv) to ADW based on
# the table name and label of the email.
def transfer_data(table_name, label):
    global user
    global pwd

    # Carlos's Paths
    download_directory = config['download_path'] + table_name 
    instant_client_path = config['instant_client_path']
    wallet_path = config['wallet_path']

    current_file = ''
    filename = ''


    try:
        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(user,pwd)

        #select the correct label and subject
        imap.select('"'+label+'"')
        resp, items2 = imap.search(None, '(SUBJECT "{sub}")'.format(sub=label))
        items = items2[0].split()

        # traverses the list returned with specific label and subject
        for i in items:
            # fetch the email message by ID
            res, msg = imap.fetch(str(int(i)), "(RFC822)")
            for response in msg:
                if isinstance(response, tuple):
                    # parse a bytes email into a message object
                    msg = email.message_from_bytes(response[1])
                    # decode the email subject
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        # if it's a bytes, decode to str
                        subject = subject.decode(encoding)
                    # decode email sender
                    From, encoding = decode_header(msg.get("From"))[0]
                    if isinstance(From, bytes):
                        From = From.decode(encoding)
                    #log("Subject:", subject)
                    #log("From:", From)
                    # if the email message is multipart
                    if msg.is_multipart():
                        # iterate over email parts
                        for part in msg.walk():
                            # extract content type of email
                            content_type = part.get_content_type()
                            content_disposition = str(part.get("Content-Disposition"))
                            try:
                                # get the email body
                                body = part.get_payload(decode=True).decode()
                                if 'TRUNCATED' in body:
                                    send_email("Truncated File in Gmail", subject+'; file has been truncated. Program exited before download.')
                                    sys.exit()
                            except:
                                pass
                            if content_type == "text/plain" and "attachment" not in content_disposition:
                                # print text/plain emails and skip attachments
                                print(body)
                            elif "attachment" in content_disposition:
                                # download attachment
                                filename = part.get_filename()
                                if filename:
                                    if not os.path.isdir(download_directory):
                                        # make a folder for this email (named after the subject)
                                        os.mkdir(download_directory)
                                    filepath = os.path.join(download_directory, filename)
                                    # download attachment and save it
                                    open(filepath, "wb").write(part.get_payload(decode=True))
                                    current_file = download_directory + '/' + filename
                    else:
                        # extract content type of email
                        content_type = msg.get_content_type()
                        # get the email body
                        body = msg.get_payload(decode=True).decode()
                        if content_type == "text/plain":
                            # log only text email parts
                            print(body)
                    print("="*100)
        # close the connection and logout
        imap.close()
        imap.logout()

    except Exception as ex:
        sendErrorEmail(ex, "Netsuite to ADW Script Error - Problem while downloading attachment from Gmail", table_name, label)
    else: log(table_name, 'Finished Downloading Attachment -')

    try:
        # log back in to delete
        imap2 = imaplib.IMAP4_SSL("imap.gmail.com")
        imap2.login(user,pwd)

        imap2.select('"'+label+'"')
        status, messages = imap2.search(None, "ALL")
        messages = messages[0].split(b' ')
        for mail in messages:
            _, msg = imap2.fetch(mail, "(RFC822)")
            # you can delete the for loop for performance if you have a long list of emails
            # because it is only for loging the SUBJECT of target email to delete
            for response in msg:
                if isinstance(response, tuple):
                    msg = email.message_from_bytes(response[1])
                    # decode the email subject
                    subject = decode_header(msg["Subject"])[0][0]
                    if isinstance(subject, bytes):
                        # if it's a bytes type, decode to str
                        subject = subject.decode()
                    log(table_name, "Deleting from Gmail")
            # mark the mail as deleted
            imap2.store(mail, "+FLAGS", "\\Deleted")

        # permanently remove mails that are marked as deleted
        # from the selected mailbox (in this case, INBOX)
        imap3 = imaplib.IMAP4_SSL("imap.gmail.com")
        imap3.login(user,pwd)
        imap3.select('"[Gmail]/All Mail"')

        imap3.expunge()
        imap2.expunge()
        # close the mailbox
        imap3.close()
        imap2.close()
        # logout from the account
        imap3.logout()
        imap2.logout()

    except Exception as ex:
        sendErrorEmail(ex, "Netsuite to ADW Script Error - Problem while deleting email after download", table_name, label)
    else: log(table_name, 'Done Deleting Email from Gmail - ')
        
    # Part 2:
    # Connect to SQL Warehouse

    # Either returns initialized path for client or continues if path already exists
    try:
        cx.init_oracle_client(lib_dir=instant_client_path)
    except cx.ProgrammingError:
        log(table_name, "Oracle Client path already created - ")
    else:
        log(table_name, "Oracle Client path now created - ")

    username = config['username_adw']
    password = config['password_adw']
    connect_string = config['connect_string_adw'] % (wallet_path)

    # downloads data and formats data for sql query
    try:
        # grabs database column names from ADW
        connection2 = cx.connect(username,password,connect_string)
        sql2 = "select COLUMN_NAME from ALL_TAB_COLUMNS where table_name = '%s' AND owner = 'DATABASE_NAME' order by column_id" % (table_name)
        #connection.version
        cursor2 = connection2.cursor()
        cursor2.execute(sql2)
        cols = []
        for row in cursor2.fetchall():
            cols.append(row[0])

        # grabs database column data types from ADW
        connection3 = cx.connect(username,password,connect_string)
        sql3 = "select DATA_TYPE from ALL_TAB_COLUMNS where table_name = '%s' AND owner = 'DATABASE_NAME' order by column_id" % (table_name)
        #connection.version
        cursor3 = connection3.cursor()
        cursor3.execute(sql3)
        types = []
        for row in cursor3.fetchall():
            types.append(row[0])

        sql_commands = ['Name', 'Date', 'Complete', 'Queue', 'TYPE', 'APPLICATION', 'CLASS', 'PARENT', 'NAME', 'ID']
        # creates column names list for query with mapped nums list for csv columns
        col_names = ['\"'+c+'\"' if c in sql_commands else c for c in cols]
        nums = [':'+str(n) for n in range(1, len(col_names)+1)]
        date_idxs = [n for n in range(len(col_names)) if 'DATE' in types[n]] # indexes of columns with DATE datatype
        internal_id_idx = [i for i in range(len(col_names)) if 'internalid' in col_names[i].lower() or 'internal_id' in col_names[i].lower()][0] #indexes of internal_id column
        print('ID Idx: ', internal_id_idx)
        print('Date Idxs: ', date_idxs)
        cursor2.close()
        connection2.close()

        # maps the col_names and nums together into one list for update clause
        maps = [col + '=' + num + ',' for col, num in zip(col_names, nums)] # e.g. "col_name=:1, internalid=:2"
        maps[len(maps)-1] = maps[len(maps)-1].strip(',') # strips last comma to avoid pl/sql syntax error
        eqs = ''.join(maps[:internal_id_idx]) + ''.join(maps[internal_id_idx+1:]) # joins into one string

    except Exception as ex:
        sendErrorEmail(ex, "Netsuite to ADW Script Error - Problem while Compiling SQL Query", table_name, label)
    else: log(table_name, 'Finished Compiling SQL Query - ')
            

    # sends data to ADW
    try:
        #connecting to ADW
        connection = cx.connect(username,password,connect_string)
        cursor = connection.cursor()
        
        # opening downloaded csv file
        with open(current_file, "r") as csv_file:
            csv_reader = csv.reader(csv_file) 
            next(csv_reader)
            data = list(csv_reader)
            sql_data = format_data(data, internal_id_idx, date_idxs) #formats data to sql standards
            
            # takes the sql query data from above (lines 253 - 296) and compiles sql query
            sql= 'MERGE INTO ' + table_name + ' v USING Dual ON (' + col_names[internal_id_idx] + '=' + nums[internal_id_idx] + ') \
            WHEN MATCHED THEN UPDATE SET ' + eqs + ' \
            WHEN NOT MATCHED THEN INSERT (' + ''.join([c+',' if idx != len(col_names)-1 else c for idx, c in enumerate(col_names)]) +') \
            VALUES (' + ''.join([c+',' if idx != len(nums)-1 else c for idx, c in enumerate(nums)]) + ')' 
            log(table_name, sql)

            # executes sql query
            try:
                cursor.executemany(sql, sql_data)       
                cursor.close()
                connection.commit()
                connection.close()
            except Exception as e:
                sendErrorEmail(e, "Netsuite to ADW Script Error - Problem with Uploading Data to Oracle", table_name, label)
    except Exception as ex:
        sendErrorEmail(ex, "Netsuite to ADW Script Error - Problem With Connecting to Oracle", table_name, label)
    else: log(table_name, 'Finished Uploading to Oracle - ')

    # renames file in directory for logging purposes
    try:
        datestr = time.strftime("%m-%d-%Y ")
        new_name = download_directory + '/' + datestr + filename
        os.rename(current_file, new_name)
    except Exception as ex:
        sendErrorEmail(ex, "Netsuite to ADW Script Error - Problem With Connecting to Oracle", table_name, label)
    else: log(table_name, 'Changed file name in directory - ')

    sep = '''
    ====================================================================================================
    ====================================================================================================
    ====================================================================================================
    '''
    log(table_name, sep)

#run program
def main():
    threads = []
    tables = ['list'.upper(), 'of'.upper(), 'tables'.upper()]
    labels = ['list', 'of', 'labels/subjects']
    # creates a thread for each csv file and runs each thread to opitmize runtime
    for t, l in zip(tables, labels):
        t = threading.Thread(target=transfer_data, args=(t, l))
        t.daemon = True
        threads.append(t)
    
    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
