import pandas as pd
import shutil 
import hl7
from datetime import date

#Copy the source files to Archive / Original folder

shutil.copy2('ADT_sample.txt', 'Archive/Original/ADT_sample.txt')
shutil.copy2('Sample ORU.txt', 'Archive/Original/Sample ORU.txt')
shutil.copy2('sampledata.csv', 'Archive/Original/sampledata.csv')

#Load the original sampledata.csv file
csv_data = pd.read_csv('sampledata.csv')

#Function to append \r to the end of each line so it can be loaded into the h17 library
def r (filename):
    new_adt_msg = ""
    file=open(filename,"r")
    adt_msg = file.readlines()
    for line in adt_msg:
        line = line.replace('\t','')
        line = line.replace('\n','')
        line += '\r'
        new_adt_msg+=line
    return new_adt_msg    

ADT = hl7.parse(r('ADT_sample.txt'))
ORU = hl7.parse(r('Sample ORU.txt'))  

# Parse the ADT record from the text file
new_adt_row = []
new_adt_row.append(str(ADT.segment('MSH')[9][0][0]) + '-' + str(ADT.segment('MSH')[9][0][1]))
new_adt_row.append(str(ADT.segment('PID')[5][0][0]))
new_adt_row.append(str(ADT.segment('PID')[5][0][1]))
new_adt_row.append(str(ADT.segment('PID')[5][0][2]))
new_adt_row.append(str(ADT.segment('PID')[11][0]).split('^')[0]) # address 1
new_adt_row.append(str(ADT.segment('PID')[11][0]).split('^')[1]) # address 2
new_adt_row.append(str(ADT.segment('PID')[11][0]).split('^')[2]) # city
new_adt_row.append(str(ADT.segment('PID')[11][0]).split('^')[3]) # state
new_adt_row.append(str(ADT.segment('PID')[11][0]).split('^')[4]) # zip
new_adt_row.append(str(ADT.segment('PID')[18][0])) #account number
new_adt_row.append(1234)

# Parse the ORU record from the text file
new_oru_row = []
new_oru_row.append(str(ORU.segment('MSH')[9][0][0]) + '-' + str(ORU.segment('MSH')[9][0][1]))
new_oru_row.append(str(ORU.segment('PID')[5][0][0]))
new_oru_row.append(str(ORU.segment('PID')[5][0][1]))
new_oru_row.append(str(ORU.segment('PID')[5][0][2]))
new_oru_row.append(str(ORU.segment('PID')[11][0]).split('^')[0]) # address 1
new_oru_row.append(str(ORU.segment('PID')[11][0]).split('^')[1]) # address 2
new_oru_row.append(str(ORU.segment('PID')[11][0]).split('^')[2]) # city
new_oru_row.append(str(ORU.segment('PID')[11][0]).split('^')[3]) # state
new_oru_row.append(str(ORU.segment('PID')[11][0]).split('^')[4]) # zip
new_oru_row.append(str(ORU.segment('PID')[18][0])) #account number
new_oru_row.append(1234)

#Columns for the new .csv files

lst_cols = ['message_type','patient_first_name','patient_last_name','patient_middle_name',
                 'patient_address_1','patient_address_2','patient_city','patient_state','patient_zip','account_number','bill_amount']

#Create new ADT and ORU csv files including records from the text files and add new columsns date_of_service and patient_name.

new_adt_ds = csv_data[csv_data['message_type'].str.startswith('ADT')][lst_cols]
new_oru_ds = csv_data[csv_data['message_type'].str.startswith('ORU')][lst_cols]
new_adt_ds.loc[len(new_adt_ds.index)] = new_adt_row
new_oru_ds.loc[len(new_oru_ds.index)] = new_oru_row
new_adt_ds[lst_cols] = new_adt_ds[lst_cols].fillna('').astype(str)
new_oru_ds[lst_cols] = new_oru_ds[lst_cols].fillna('').astype(str)
new_adt_ds['date_of_service'] = date.today()
new_oru_ds['date_of_service'] = date.today()
new_adt_ds['patient_name'] = new_adt_ds['patient_last_name'] + ' ' + new_adt_ds['patient_first_name']  + ' ' + new_adt_ds['patient_middle_name']
new_oru_ds['patient_name'] = new_oru_ds['patient_last_name'] + ' ' + new_oru_ds['patient_first_name']  + ' ' + new_oru_ds['patient_middle_name']

#Save files as .csv
new_adt_ds.to_csv('Archive/Modified/ADT_' + str(date.today()) + '_Modified_file.csv' , index=False)
new_oru_ds.to_csv('Archive/Modified/ORU_' + str(date.today()) + '_Modified_file.csv' , index=False)

#create the report .txt file
rpt = csv_data.groupby('patient_state',as_index = False)[['bill_amount']].sum()
rpt.to_csv('Archive/Modified/report.txt', sep='\t', index=False)

# Import ADT csv file into sqllite DB and fetch all records
import sqlite3
connection = sqlite3.connect('adt.db')
try:
    connection.execute("DROP TABLE adt_table")
except sqlite3.OperationalError:
    print('No table exists')

new_adt_ds.to_sql('adt_table', connection, if_exists='append', index=False)

cursor = connection.cursor()
select_all = "SELECT * FROM adt_table"
rows = cursor.execute(select_all).fetchall()
print(rows)
connection.close()