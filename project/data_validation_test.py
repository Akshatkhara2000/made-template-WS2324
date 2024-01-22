import pandas as pd
from sqlalchemy import create_engine
import os
import re
import sqlite3
import opendatasets as od
import ipdb

# class Test:
def Test_file(Sqlfilepath, tablename):
    # Connect to SQLite database
    conn = sqlite3.connect(Sqlfilepath)

    # Test case: Selecting the query using PANDAS function to check whether the data is stored in the database.
    query = f"SELECT * FROM {tablename};"
    df = pd.read_sql_query(query, conn)
    # print(df.columns)
    if len(df)>1:
        print("Data exists in SQL file located in the data folder")
    else:
        print("Data does not exist in SQL file")
    # print(df.head())

    # Close the connection
    conn.close()

def Check_null(Sqlfilepath, tablename):
    # Connect to SQLite database
    conn = sqlite3.connect(Sqlfilepath)

    # Test case: Selecting the query using PANDAS function to check whether the data is stored in the database.
    query = f"SELECT * FROM {tablename}"
    df = pd.read_sql_query(query, conn)
    if df.isnull().any().any():
        print("DataFrame contains null values.")
    else:
        print("DataFrame does not contain null values.")
    # print(df.columns)

    # Close the connection
    conn.close()

def Test_dataset():
    #Step 1 Check Zomato dataset availabke in folder data
    print("Testing if data exists in Zomato dataset Banglore_1")
    Sqlfilepath1 = os.path.join(os.getcwd(), "data", "Zomato.sqlite")
    tablename1 = "Zomato"
    Test_file(Sqlfilepath1, tablename1)
    print("Test run 1.1 complete for zomato dataset")

    #Step 2 Check Zomato dataset availabke in folder data
    print("Testing to check null values in Zomato dataset Banglore_1")
    Check_null(Sqlfilepath1, tablename1)
    print("Test run 1.2 complete for zomato dataset")

    #Step 3 Check Zomato banglore dataset availabke in folder data
    print("Testing if data exists in Zomato dataset Banglore_2")
    Sqlfilepath2 = os.path.join(os.getcwd(), "data", "Banglore.sqlite")
    tablename2 = "Banglore"
    Test_file(Sqlfilepath2, tablename2)
    print("Test run 2.1 complete for zomato banglore dataset")

    #Step 4 Check Zomato banglore dataset availabke in folder data
    print("Testing to check null values in Zomato dataset Banglore_2")
    Sqlfilepath = os.path.join(os.getcwd(), "data", "Banglore.sqlite")
    tablename2 = "Banglore"
    Check_null(Sqlfilepath2, tablename2)
    print("Test run 2.2 complete for zomato banglore dataset")
    
    print("All Test runs Completed :)")

if __name__ == "__main__":
    Test_dataset()