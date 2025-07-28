import pandas as pd
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
    
# define where we are getting these files from
customer_filepath = '../data/03_Library SystemCustomers.csv'
book_filepath = '../data/03_Library Systembook.csv'


# import them into data frames

customer = pd.read_csv(customer_filepath)
book = pd.read_csv(book_filepath)

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

invalid_checkoutdate = book[book['BookCheckout'] >= pd.Timestamp.today()]
invalid_returndate = book[book['BookReturned'] >= pd.Timestamp.today()]
null_checkoutdate = book[book['BookCheckout'].isna()]
null_returndate = book[book['BookReturned'].isna()]

invalid_dates = pd.concat([invalid_checkoutdate,invalid_returndate,null_checkoutdate,null_returndate], ignore_index = True)

print (book)
print (customer)

print (book_duplicates)
print (customer_duplicates)
print (invalid_dates)

book.to_csv('../data/book.csv')
customer.to_csv('../data/customer.csv')
invalid_dates.to_csv('../data/invalid_books.csv')