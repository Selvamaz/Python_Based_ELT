from datetime import datetime
from datetime import date
import random
import time
import os
import openpyxl
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows
from babel.numbers import format_currency
import datetime


invoice_id = random.randrange(100000,999999)
branch = random.choice(['Karur','Namakkal','Salem','Chennai','Coimbatore','Trichy'])
gender = random.choice(['Female','Male'])
datefk = date.today().strftime("%m-%d-%y")
timefk = time.strftime("%H:%M")
total = random.randrange(50,10000)
tax = total * 0.18
final = total + tax
delivery = random.choice(['On Store','Courier'])
income = total * 0.05

df = pd.DataFrame({'Invoice ID' : invoice_id, 'Branch' : branch, 'Date' : datefk, 'Time' : timefk, 'Gender' : gender,'Total' : format_currency(total, 'INR', locale='en_IN'), 'Tax (18%)' : format_currency(tax, 'INR', locale='en_IN'), 'Final' : format_currency(final, 'INR', locale='en_IN'), 'Delivery' : delivery, 'Income (5%)' : format_currency(income, 'INR', locale='en_IN')}, index=[1])

file = r"Super_Market_Sales.xlsx"

if os.path.isfile(file):  # if file already exists append to existing file
    workbook = openpyxl.load_workbook(file)  # load workbook if already exists
    sheet = workbook['Sheet1']  # declare the active sheet 

    # append the dataframe results to the current excel file
    for row in dataframe_to_rows(df, header = False, index = False):
        sheet.append(row)
    workbook.save(file)  # save workbook
    workbook.close()  # close workbook
    print("\n\n\n Updated new entries !!!\n\n\n")
else:  # create the excel file if doesn't already exist
    with pd.ExcelWriter(path = file, engine = 'openpyxl') as writer:
        df.to_excel(writer, index = False, sheet_name = 'Sheet1')
    print("\n\n\n Created a new excel !!!\n\n\n")
