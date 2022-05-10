import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pyhive import hive
import configuration as c

#Hive Settings============================================================================#
hive_cnx = hive.Connection(host=c.hdfs_host,port=c.hive_port,username=c.hive_username,password=c.hive_password,database=c.hive_database,auth=c.hive_mode)

#Query Hive================================================================================#
cursor1 = hive_cnx.cursor()
cursor1.execute('select vendorid ,round(sum(total_amount)) amount  from traffic group by vendorid')
data = cursor1.fetchall()
cursor1.close()

result1 = ''
for i in range(len(data)):
    df_data = "Vendor ID Number: " + str(data[i][0]), "Total Amount (USD): " + str(data[i][1])
    result1 = str(result1) + str(df_data) + '\n'
    result1 = result1.replace("(", "").replace(")", "").replace("'", "").replace(",", "   :   ")

    # result1 = result1.replace(")", "")
    # result1 = result1.replace("'", "")
    # result1 = result1.replace(",", "   :   ")

#Email Settings========================================================================#
port = 587
smtp_server = "smtp.gmail.com"
message = MIMEMultipart("alternative")
message["Subject"] = "Taxi Station Daily Report"
message["From"] = c.email
message["To"] = c.email

#Email Content=========================================================================#
# write the plain text part
text1 = """\
Hi Alin :)
 Good morning, 
we sending you the daily status as you ask : 
please see below where your taxi vendors stands in term of total revenue.\n
"""
text = text1 + result1

#Sending Email========================================================================#
# convert both parts to MIMEText objects and add them to the MIMEMultipart message
part1 = MIMEText(text, "plain")
message.attach(part1)

#send your email=======================================================================#
with smtplib.SMTP("smtp.gmail.com", 587) as server:
    server.ehlo()
    server.starttls()
    server.login(c.email, c.password)
    server.sendmail( c.email, c.email, message.as_string())
print('Done')
