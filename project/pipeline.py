import pandas as pd
from sqlalchemy import create_engine
import os
import sqlite3
import opendatasets as od
import shutil

# class DataPipeline:

def download_csv_files():
    dataset_path = os.path.join(os.getcwd(), "data")
    banglore_csv = os.path.join(os.getcwd(), "data/BangaloreZomatoData.csv")

    # Download Zomato Dataset 1
    source_path1 = os.path.join(os.getcwd(), "zomato-dataset")
    zomato_csv = os.path.join(os.getcwd(), "data/zomato.csv")
    od.download('https://www.kaggle.com/datasets/rajeshrampure/zomato-dataset/data')

    # Gather all files
    allfiles = os.listdir(source_path1)
    
    # iterate on all files to move them to destination folder
    for f in allfiles:
        src_path = os.path.join(source_path1, f)
        dst_path = os.path.join(dataset_path, f)
        shutil.move(src_path, dst_path)

    zomato_banglore_1 = pd.read_csv(zomato_csv)
    zomato_banglore_1_df = pd.DataFrame(zomato_banglore_1)
    shutil.rmtree("zomato-dataset")

    # Download Zomato Banglore Dataset 2
    source_path2 = os.path.join(os.getcwd(), "zomato-bangalore-restaurants-2022")
    zomato_csv = os.path.join(os.getcwd(), "data/zomato.csv")
    od.download('https://www.kaggle.com/datasets/vora1011/zomato-bangalore-restaurants-2022/data')
    # Gather all files
    allfiles = os.listdir(source_path2)
    
    # iterate on all files to move them to destination folder
    for f in allfiles:
        src_path = os.path.join(source_path2, f)
        dst_path = os.path.join(dataset_path, f)
        shutil.move(src_path, dst_path)
    
    zomato_banglore_2 = pd.read_csv(banglore_csv)
    zomato_banglore_2_df = pd.DataFrame(zomato_banglore_2)
    shutil.rmtree("zomato-bangalore-restaurants-2022")
    
    return zomato_banglore_1_df, zomato_banglore_2_df

def Zomato_banglore_1(dataframe):

    # import ipdb; ipdb.set_trace()

    zomato_banglore_1_cleaned_df = dataframe
    zomato_banglore_1_cleaned_df.dropna(inplace=True)
    zomato_banglore_1_cleaned_df = zomato_banglore_1_cleaned_df.drop(["url", "address", "phone", "reviews_list", "menu_item"], axis='columns')

    # Online_order and Book_table column cleaning
    zomato_banglore_1_cleaned_df = zomato_banglore_1_cleaned_df[zomato_banglore_1_cleaned_df['online_order'].isin(['Yes','No'])]
    zomato_banglore_1_cleaned_df = zomato_banglore_1_cleaned_df[zomato_banglore_1_cleaned_df['book_table'].isin(['Yes','No'])]
    
    # Rate column cleaning
    zomato_banglore_1_cleaned_df['rate'] = zomato_banglore_1_cleaned_df['rate'].str.replace('/5', '')
    zomato_banglore_1_cleaned_df['rate'] = zomato_banglore_1_cleaned_df['rate'].str.replace('\W', '', regex=True)
    zomato_banglore_1_cleaned_df['rate'] = zomato_banglore_1_cleaned_df['rate'].str.replace('\D', '', regex=True)
    # zomato_banglore_1_cleaned_df['rate'] = zomato_banglore_1_cleaned_df['rate'].astype(int)

    return zomato_banglore_1_cleaned_df
    
def Zomato_database_file(dataframe):
    Zomato_banglore_cleaned = dataframe
    # Connect to SQLite database
    Sqlfilepath = os.path.join(os.getcwd(), "data", "Zomato.sqlite")
    conn = sqlite3.connect(Sqlfilepath)
    # Use the to_sql method to write the DataFrame to a SQLite table
    Zomato_banglore_cleaned.to_sql('Zomato', conn, index=False, if_exists='replace')

    # Close the connection
    conn.close()


def Zomato_banglore_2(dataframe):

    # import ipdb; ipdb.set_trace()

    zomato_banglore_2_cleaned_df = dataframe

    zomato_banglore_2_cleaned_df.dropna(inplace=True)

    zomato_banglore_2_cleaned_df = zomato_banglore_2_cleaned_df.drop(["URL","Timing", "Full_Address", "PhoneNumber", "PeopleKnownFor"], axis='columns')
    
    # zomato_banglore_2_cleaned_df = zomato_banglore_2_cleaned_df.loc[zomato_banglore_2_cleaned_df['IsHomeDelivery'] == 1, 'IsHomeDelivery'] = "Yes"
    for x in zomato_banglore_2_cleaned_df.index:
        if zomato_banglore_2_cleaned_df.loc[x, "IsHomeDelivery"] == 1:
            zomato_banglore_2_cleaned_df.loc[x, "IsHomeDelivery"] = "Yes"
        elif zomato_banglore_2_cleaned_df.loc[x, "IsHomeDelivery"] == 0:
            zomato_banglore_2_cleaned_df.loc[x, "IsHomeDelivery"] = "No"
    
    for x in zomato_banglore_2_cleaned_df.index:
        if zomato_banglore_2_cleaned_df.loc[x, "isTakeaway"] == 1:
            zomato_banglore_2_cleaned_df.loc[x, "isTakeaway"] = "Yes"
        elif zomato_banglore_2_cleaned_df.loc[x, "isTakeaway"] == 0:
            zomato_banglore_2_cleaned_df.loc[x, "isTakeaway"] = "No"
    
    for x in zomato_banglore_2_cleaned_df.index:
        if zomato_banglore_2_cleaned_df.loc[x, "isVegOnly"] == 1:
            zomato_banglore_2_cleaned_df.loc[x, "isVegOnly"] = "Yes"
        elif zomato_banglore_2_cleaned_df.loc[x, "isVegOnly"] == 0:
            zomato_banglore_2_cleaned_df.loc[x, "isVegOnly"] = "No"

    return zomato_banglore_2_cleaned_df

    # # Remove All Numbers from Strings
    # zomato_banglore_2_cleaned_df['my_column'] = zomato_banglore_2_cleaned_df['my_column'].str.replace('\d+', '', regex=True)

    # # Remove Special Characters
    # zomato_banglore_2_cleaned_df['my_column'] = zomato_banglore_2_cleaned_df['my_column'].str.replace('\W', '', regex=True)

    
def Zomato_database_file_2(dataframe):
    Zomato_banglore_cleaned = dataframe
    # Connect to SQLite database
    Sqlfilepath = os.path.join(os.getcwd(), "data", "Banglore.sqlite")
    conn = sqlite3.connect(Sqlfilepath)
    # Use the to_sql method to write the DataFrame to a SQLite table
    Zomato_banglore_cleaned.to_sql('Banglore', conn, index=False, if_exists='replace')

    # Close the connection
    conn.close()

def data_pipeline():
    print("Downloading Datasets... ")
    zomato_banglore_1_df, zomato_banglore_2_df = download_csv_files()
    print("Download complete")

    #Step 2 Clean zomato banglore dataset 1 and Load into SQLite file
    print("Cleaning Zomato dataset Banglore_1")
    zomato_banglore_1_cleaned = Zomato_banglore_1(zomato_banglore_1_df)
    print("Zomato data cleaned loading into SQL Lite database")
    Zomato_database_file(zomato_banglore_1_cleaned)
    print("Loaded data into SQL file successfully")

    #Step 3 Clean zomato banglore dataset 2 and Load into SQLite file
    print("Cleaning Zomato dataset Banglore_2")
    zomato_banglore_2_cleaned = Zomato_banglore_2(zomato_banglore_2_df)
    print("Zomato data cleaned loading into SQL Lite database")
    Zomato_database_file_2(zomato_banglore_2_cleaned)
    print("Loaded data into SQL file successfully")
    
    print("All Tasks Completed :) ")

if __name__ == "__main__":
    data_pipeline()
