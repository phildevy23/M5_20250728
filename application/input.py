import pandas as pd
from sqlalchemy import create_engine, text
import urllib
from openai import OpenAI
from dotenv import load_dotenv
import os 
import time
from datetime import datetime, timedelta
from pathlib import Path

metrics = {
"run_id" : 0,
"number_of_customer_rows_dropped": 0,
"number_of_book_rows_dropped" : 0,
"number_of_book_na": 0,
"number_of_customer_na": 0,
"numer_of_invalid_dates": 0,
"number_of_OpenAI_API_calls": 0,
"number_of_customers" : 0,
"total_run_duration": 0.0,
"initialisation_step_duration": 0.0,
"import_step_duration" : 0.0,
"bronze_write_duration" : 0.0,
"cleaning_step_duration" : 0.0,
"Open_AI_step_duration" : 0.0,
"export_duration" : 0.0
}


def string_to_duration(duration_string):
    number_str,unit_str = duration_string.split(' ',1)
    number_int = int(number_str)
    if unit_str.lower().strip() in ['week','weeks']:
        new_duration = number_int * 7
    elif unit_str.lower().strip() in ['day','days']:
        new_duration = number_int * 1
    else:
        new_duration = 0
    return new_duration
#commit

class TitleCleaner:

    def __init__(self):
        load_dotenv()
        print("Obtaining OpenAI API Key")
        self.client = OpenAI(api_key = os.environ.get("OPENAI_API_KEY"))

    def clean_book_title(self,title): # send book title to openAI and get a Corrected and Formatted Result
        prompt = f"""be a helpful assistant, the following is a misspelled or slightly incorrect book title. Correct it and return the proper title, as it would appear in a bookstore catalogue. If its all ready correct then return it as-is. only return the correct book title, nothing else. return a blank string if you cannot process the input
    Incorrect Title: "{title}"
    Correct Title: """

        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages = [{"role": "system" , "content" : "You are an assistant that corrects and standardises book titles. only return the corrected book title. No Quotes, explanations or formatting. "},{"role":"user", "content":prompt}],temperature=0.3
        )
        corrected = response.choices[0].message.content.strip().strip('"')
        print (f"OpenAI identified '{title}' as '{corrected}'.")
        metrics['number_of_OpenAI_API_calls'] += 1
        return corrected

def Import_File(filename: str):
    script_dir = Path(__file__).resolve().parent
    print (f"Attempting to Import {script_dir.parent / 'data' / filename}")
    return pd.read_csv(script_dir.parent / 'data' / filename)


class SQLHelper:

    def __init__(self,DB_Version):
        # define database paramaters
        if DB_Version == 'prod':
            database_params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=staging;"
            "Trusted_Connection=yes;"
            )

        elif DB_Version == 'test': 
            database_params =   urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=192.168.0.100;"
            "DATABASE=staging;"
            "UID=sa;"
            "PWD=ChangeMe!;"
        )

        #create a database engine instance 
        print (f"Creating {DB_Version} SQL Engine")
        self.engine = create_engine(f'mssql+pyodbc:///?odbc_connect={database_params}')

    def Drop_SQL_Table(self,tablename: str):
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"drop table if exists {tablename}"))

    def Write_to_SQL(self, source: pd.DataFrame, destination: str, drop = 1):
        #write to sql
        if drop == 1:
            self.Drop_SQL_Table(destination)
        print (f"Writing SQL to {destination}")
        source.to_sql(destination,con=self.engine,if_exists='append',index=False)



def Write_to_CSV(source:pd.DataFrame, destination: str):

    script_dir = Path(__file__).resolve().parent
    print (f"Writing CSV to {script_dir.parent} / 'data' / {destination}")
    return source.to_csv(script_dir.parent / 'data' / destination)

def Remove_Null_Values(source: pd.DataFrame):
    print(f"Removed Null rows")
    source.dropna(how ='all',inplace=True)

def Capture_Duplicates(source: pd.DataFrame,detection_columns: list[str] = None):
    print(f"Capturing Duplicates")
    return source[source.duplicated(subset = detection_columns)]

def Drop_Duplicates(source: pd.DataFrame,detection_columns: list[str] = None):
    print (f"Dropping duplicates")
    return source.drop_duplicates(subset = detection_columns, inplace=True)

def Rename_Column(source: pd.DataFrame, original: str, new: str):
    print (f"Renaming Column in dataframe:  {original} is now {new}.")
    source.rename(columns={original : new}, inplace=True)

def Clean_Date(source: pd.DataFrame, column : str):
    print (f"Cleaning Date from {column}")
    source[column] = source[column].str.replace('"','').str.strip()
# Clean date columns of quotation marks

def Convert_to_Int(source: pd.DataFrame, column: str):
    print(f"Converted {column} to an Integer")
    source[column] = source[column].fillna(0).astype(int)

def Convert_to_Date(source: pd.DataFrame, column:str):
    print(f"Converted {column} to an Date")
    source[column] = pd.to_datetime(source[column],errors='coerce',dayfirst=True)

def Calculate_date_Difference(source:pd.DataFrame, date1: str, date2: str):
    difference = (source[date1] - source[date2]).dt.days.fillna(0).astype(int)
    print (f"Calculated difference between {date1} and {date2} as {difference} days.")
    return difference

def Capture_Invalid_Dates(source: pd.DataFrame, column: str):
    print(f"Capturing Invalid dates from {column}")
    return source[(source[column] > pd.Timestamp.today()) | (source[column].isna())]
    

if __name__ == '__main__':
    start_time = time.time()
    # Initialisation --------------------------------------
    SQL_Database = SQLHelper('test')
    OpenAI_Client = TitleCleaner()

    initialisation_time = time.time()
    # Import data from CSV ---------------------------------
    customer = Import_File('03_Library SystemCustomers.csv')
    book = Import_File('03_Library Systembook.csv')
    
    import_time = time.time()
    # Write to bronze layer ------------------------------- 
    print ("-Writing Bronze Layer Data to SQL")
    SQL_Database.Write_to_SQL(customer,'customer_bronze')
    SQL_Database.Write_to_SQL(book,'book_bronze')
    # Begin Cleaning Data -------------------------------------------------------------
    bronze_write_time = time.time()

    print ("-Cleaning and Validating Data")
    
    #Remove Null values from Customers and Books
    customer_len = len(customer)
    book_len = len(book)

    Remove_Null_Values(customer)
    Remove_Null_Values(book)
    customer_dropped =  customer_len - len(customer)
    book_dropped = book_len - len(book)

    # capture and drop Duplicates from customer and books. Exclude the Incremental ID from the book Dupliate check
    customer_duplicates = Capture_Duplicates(customer)
    Drop_Duplicates(customer)

    book_duplicates = Capture_Duplicates(book,['Books','Book checkout','Book Returned','Days allowed to borrow','Customer ID'])
    Drop_Duplicates(book,['Books','Book checkout','Book Returned','Days allowed to borrow','Customer ID'])

    # Rename Customer Columns
    Rename_Column(customer,'Customer ID','CustomerID')
    Rename_Column(customer,'Customer Name','Customer Name')

    #Rename Book Columns
    Rename_Column(book,'Book checkout','BookCheckout')
    Rename_Column(book,'Book Returned','BookReturned')
    Rename_Column(book,'Id','LoanID')
    Rename_Column(book,'Books','BookTitle')
    Rename_Column(book,'Days allowed to borrow','RentalPeriodText')
    Rename_Column(book,'Customer ID','CustomerID')

    # Clean the Date Fields in the book table
    Clean_Date(book,'BookCheckout')
    Clean_Date(book,'BookReturned')

    # convert data types, IDs to INT and Dates to DATE

    Convert_to_Int(customer,'CustomerID')
    Convert_to_Int(book,'CustomerID')    
    Convert_to_Int(book,'LoanID')

    Convert_to_Date(book,'BookReturned')
    Convert_to_Date(book,'BookCheckout')
    

    #Trim book name
    book['BookTitle'] = book['BookTitle'].str.strip()

    #calucalted column for rental_duration 
    book['RentalDays'] = book['RentalPeriodText'].apply(string_to_duration)

    # calculate column for return date (checkout date + rental days allowed)
    book['ReturnDate'] = pd.to_datetime(book['BookCheckout']) + pd.to_timedelta(book['RentalDays'], unit='D')

    # calculate column for dayslate (days between checkout date and return date)
    book['DaysLate'] = (pd.to_datetime(book['BookReturned']) - pd.to_datetime(book['BookCheckout'])).dt.days

    # capture any invalid data and store for seperate processing
    invalid_checkoutdate = Capture_Invalid_Dates(book,'BookCheckout')
    invalid_returndate = Capture_Invalid_Dates(book,'BookReturned')
    invalid_dates = pd.concat([invalid_checkoutdate,invalid_returndate], ignore_index = True) # build a list of all invalid dates

    cleaning_time = time.time()

        # calculate column to clean title (Send to OpenAI model)
    print ("Sending titles to OpenAI")
    book['CorrectedTitle'] = book['BookTitle'].apply(lambda title: OpenAI_Client.clean_book_title(title))
    open_ai_time = time.time()

    print ("-Writing output to CSV")
    #write results to csv 
    Write_to_CSV(book,'book.csv')
    Write_to_CSV(customer,'customer.csv')
    Write_to_CSV(invalid_dates,'invalid_books.csv')
    Write_to_CSV(book_duplicates,'book_duplicates.csv')
    Write_to_CSV(customer_duplicates,'customer_duplicates.csv')

    #write silver results to SQL
    print ("-Writing output to SQL")
    SQL_Database.Write_to_SQL(book,'book_silver')
    SQL_Database.Write_to_SQL(customer,'customer_silver')
    SQL_Database.Write_to_SQL(invalid_dates,'book_date_errors')
    SQL_Database.Write_to_SQL(book_duplicates,'book_duplicate_errors')
    SQL_Database.Write_to_SQL(customer_duplicates,'customer_duplicate_errors')

    end_time = time.time()

    metrics['run_id'] = datetime.now().strftime("%Y%m%d%H%M%S")
    metrics['number_of_customers'] = len(customer)
    metrics['number_of_book_na'] = book_dropped
    metrics['number_of_customer_na'] = customer_dropped
    
    metrics['number_of_customer_rows_dropped'] = len(customer_duplicates)
    metrics['number_of_book_rows_dropped'] = len(book_duplicates)
    metrics['numer_of_invalid_dates'] = len(invalid_dates)

    metrics['initialisation_step_duration'] = initialisation_time - start_time
    metrics['import_step_duration'] = import_time - initialisation_time
    metrics['bronze_write_duration'] = bronze_write_time - import_time
    metrics['cleaning_step_duration'] = cleaning_time - bronze_write_time
    metrics['Open_AI_step_duration'] = open_ai_time - cleaning_time
    metrics['export_duration'] = end_time - open_ai_time

    metrics['total_run_duration'] = end_time - start_time
    metrics_df = pd.DataFrame([metrics])
    SQL_Database.Write_to_SQL(metrics_df,'run_metrics',0)

    print ("Processing Complete")
