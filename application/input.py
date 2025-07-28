import pandas as pd
from sqlalchemy import create_engine
import urllib
from openai import OpenAI
from dotenv import load_dotenv
import os 
from datetime import datetime, timedelta

def string_to_duration(duration_string):
    number_str,unit_str = duration_string.split(' ',1)
    print (number_str)
    print (unit_str)
    number_int = int(number_str)
    if unit_str.lower().strip() in ['week','weeks']:
        new_date = number_int * 7
    elif unit_str.lower().strip() in ['day','days']:
        new_date = number_int * 1
    else:
        new_date = 0
    return new_date
#commit

def clean_book_title(title):
    prompt = f"""be a helpful assistant, the following is a misspelled or slightly incorrect book title. Correct it and return the proper title, as it would appear in a bookstore catalogue. If its allready correct then return it as-is. only return the correct book title, nothing else. return a blank string if you cannot process the input
Incorrect Title: "{title}"
Correct Title: """

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages = [{"role": "system" , "content" : "You are an assistant that corrects and standardises book titles. only return the corrected book title. No Quotes, explanations or formatting. "},{"role":"user", "content":prompt}],temperature=0.3
    )
    corrected = response.choices[0].message.content.strip().strip('"')
    return corrected

# define where we are getting these files from
customer_filepath = '../data/03_Library SystemCustomers.csv'
book_filepath = '../data/03_Library Systembook.csv'

load_dotenv()
client = OpenAI(api_key = os.environ.get("OPENAI_API_KEY"))

# define database paramaters
database_params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=staging;"
    "Trusted_Connection=yes;"
)

#create a database engine instance 

database_engine = create_engine(f'mssql+pyodbc:///?odbc_connect={database_params}')


# import them into data frames

customer = pd.read_csv(customer_filepath)
book = pd.read_csv(book_filepath)

#write the bronze data

book.to_sql('book_bronze',con=database_engine,if_exists='append',index=False)
customer.to_sql('customer_bronze',con=database_engine,if_exists='append',index=False)

# print basic data about them
print ("Customer")
print (customer)

print ("Book")
print(book)


#Required Transformations
#Remove nullls
#Rename Columns in camelcase
#Create list of duplicates
#Customers.CustomerID to INT
#Books.Id to INT
#Books.checkout to Date
#Books.returned to Date
#Create column from Books.day allowed to borrow


#Remove Null values from Customers 
customer.dropna(how ='all',inplace=True)
print(customer)

#Remove Null values from book
book.dropna(how='all',inplace=True)
print(book)

# capture and drop duplicates

customer_duplicates = customer[customer.duplicated()]
customer.drop_duplicates(inplace =True)

book_duplicates = book[book.duplicated(subset=['Books','Book checkout','Book Returned','Days allowed to borrow','Customer ID'])]
book.drop_duplicates(subset=['Books','Book checkout','Book Returned','Days allowed to borrow','Customer ID'],inplace=True)
#Rename customer columns

customer.rename(columns={'Customer ID':'CustomerID'}, inplace=True)
customer.rename(columns={'Customer Name': 'Customer Name'}, inplace = True)

#Rename Book Columns

book.rename(columns={'Book checkout':'BookCheckout'}, inplace=True)
book.rename(columns={'Book Returned':'BookReturned'}, inplace=True)
book.rename(columns={'Id':'LoanID'}, inplace=True)
book.rename(columns={'Books':'BookTitle'}, inplace=True)
book.rename(columns={'Days allowed to borrow':'RentalPeriodText'}, inplace=True)
book.rename(columns={'Customer ID':'CustomerID'}, inplace=True)


print ('Columns Renamed')
print(book.columns.to_list())


# Clean date columns of quotation marks

book['BookCheckout'] = book['BookCheckout'].str.replace ('"','')
book['BookReturned'] = book['BookReturned'].str.replace ('"','')

# trim date columns

book['BookCheckout'] = book['BookCheckout'].str.strip ()
book['BookReturned'] = book['BookReturned'].str.strip ()

# convert data types

customer['CustomerID'] = customer['CustomerID'].fillna(0).astype(int) # convert customer id to int
book['LoanID'] = book['LoanID'].fillna(0).astype(int) #convert customer id to int
book['CustomerID'] = book['CustomerID'].fillna(0).astype(int) #convert customer id to int
book['BookReturned'] = pd.to_datetime(book['BookReturned'],errors='coerce',dayfirst=True) # convert returned date to a datetype
book['BookCheckout'] = pd.to_datetime(book['BookCheckout'],errors='coerce',dayfirst=True) # convert checkout date to a datetype


#trim book name

book['BookTitle'] = book['BookTitle'].str.strip()

#calucalted column for rental_duration 
book['RentalDays'] = book['RentalPeriodText'].apply(string_to_duration)
book['ReturnDate'] = book['BookCheckout'] + pd.to_timedelta(book['RentalDays'], unit ='D')
book['DaysLate'] = (book['BookReturned']- book['BookCheckout']).dt.days.fillna(0).astype(int)
book['CorrectedTitle'] = book['BookTitle'].apply(clean_book_title)

invalid_checkoutdate = book[book['BookCheckout'] >= pd.Timestamp.today()] # identify dates in the future
invalid_returndate = book[book['BookReturned'] >= pd.Timestamp.today()]
null_checkoutdate = book[book['BookCheckout'].isna()] # identify null dates
null_returndate = book[book['BookReturned'].isna()]

invalid_dates = pd.concat([invalid_checkoutdate,invalid_returndate,null_checkoutdate,null_returndate], ignore_index = True) # build a list of all invalid dates


#write results to csv 
book.to_csv('../data/book.csv')
customer.to_csv('../data/customer.csv')
invalid_dates.to_csv('../data/invalid_books.csv')

#write silver results to SQL

book.to_sql('book_silver',con=database_engine,if_exists='append',index=False)
customer.to_sql('customer_silver',con=database_engine,if_exists='append',index=False)
invalid_dates.to_sql('book_silver_errors',con=database_engine,if_exists='append',index=False)