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
import sys
from prefect import flow, task
os.system('cls')

@flow
def createrows():
    data = []
    try:
        max_rows =1000
        from_year = 2020
        end_year = 2023
    except:
        sys.exit("Error !!! Please give integer number !!!")
        
    for i in range(max_rows):
        invoice_id = random.randrange(100000,999999)
        branch = random.choice(['Karur','Namakkal','Salem','Chennai','Coimbatore','Trichy'])
        gender = random.choice(['Female','Male'])
        datestring = "" + str(random.randrange(1,12)) + "-" + str(random.randrange(1,28)) + "-" + str(random.randrange(from_year,end_year))
        datefk = datetime.datetime.strptime(datestring, '%m-%d-%Y').date()
        timestring = "" + str(random.randrange(0,24)) + ":" + str(random.randrange(0,60))
        timefk = datetime.datetime.strptime(timestring, '%H:%M').time()
        total = random.randrange(50,10000)
        tax = total * 0.18
        final = total + tax
        delivery = random.choice(['On Store','Courier'])
        income = total * 0.05
        data.append({'Invoice ID' : invoice_id, 'Branch' : branch, 'Date' : datefk, 'Time' : timefk, 'Gender' : gender,'Total' : format_currency(total, 'INR', locale='en_IN'), 'Tax (18%)' : format_currency(tax, 'INR', locale='en_IN'), 'Final' : format_currency(final, 'INR', locale='en_IN'), 'Delivery' : delivery, 'Income (5%)' : format_currency(income, 'INR', locale='en_IN')})

    df = pd.DataFrame(data)
    filename = 'Source File - Excel.xlsx'

    try:
        if os.path.exists(filename):
            with pd.ExcelWriter(filename, mode='a', engine = 'openpyxl', if_sheet_exists='overlay') as writer:
                df.to_excel(writer, index = False, sheet_name = 'Sheet1')
            print("\n\n\nExcel Entries Successfull !!!\n\n\n")
        else:
            with pd.ExcelWriter(filename, engine = 'openpyxl') as writer:
                df.to_excel(writer, index = False, sheet_name = 'Sheet1')
            print("\n\n\nNew Excel Created and Entries Successfull !!!\n\n\n")
    except Exception as err:
        print("\n\nError writing/creating/updating excel !!!\n", f"{type(err).__name__} was raised: {err}")

if __name__ == "__main__":
    createrows.serve(name="Faker_Deployment", tags=['Scheduled'], interval=600)