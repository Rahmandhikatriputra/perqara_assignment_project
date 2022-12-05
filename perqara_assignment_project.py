#%% import all important libraries
import os
import pandas as pd
from datetime import datetime
import psycopg2

#%% Extract
start_time_etl = datetime.now()

# insert file directory where all files are stored
file_dir = "C:/work/perqara/pre_work/20220721_Data_Assesment Data/data"

# list all file names
files_name = os.listdir(file_dir)

# Read all files and put them into one dictionary
dir_file = {}

for file in files_name:
    dir_file[file] = pd.read_csv("{}/{}".format(file_dir, file))
    
#%% Transform

# Change data type according to its type
## order_items_dataset
order_items_dataset = dir_file["order_items_dataset.csv"]
    
order_items_dataset["shipping_limit_date"] = order_items_dataset["shipping_limit_date"].apply(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))

dir_file["order_items_dataset.csv"] = order_items_dataset
    
## order_reviews_dataset
order_reviews_dataset = dir_file["order_reviews_dataset.csv"]
    
order_reviews_dataset["review_creation_date"] = order_reviews_dataset["review_creation_date"].apply(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
order_reviews_dataset["review_answer_timestamp"] = order_reviews_dataset["review_answer_timestamp"].apply(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    
dir_file["order_reviews_dataset.csv"] = order_reviews_dataset

## order_dataset
orders_dataset = dir_file["orders_dataset.csv"]

list_column = orders_dataset.columns
    
for column in list_column[3:]:
    orders_dataset[column] = orders_dataset[column].apply(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S") if type(x) != type(12.32) else x)
    
dir_file["orders_dataset.csv"] = orders_dataset

# Change the null values into an appropriate value if possible
## order_reviews_dataset
order_reviews_dataset = dir_file["order_reviews_dataset.csv"]

list_column = order_reviews_dataset.columns

for column in list_column[3:5]:
    order_reviews_dataset[column] = order_reviews_dataset[column].apply(lambda x : "Not inputted by user" if type(x) == type(123.132) or x.isspace() else x)
    
dir_file["order_reviews_dataset.csv"] = order_reviews_dataset

# create datamart for marketing team
## Upload all necessary datasets
order_reviews_dataset = dir_file["order_reviews_dataset.csv"][["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_answer_timestamp"]]
orders_dataset = dir_file["orders_dataset.csv"]
order_items_dataset = dir_file["order_items_dataset.csv"][["order_id", "product_id"]]
customers_dataset = dir_file["customers_dataset.csv"].drop(columns = ["customer_unique_id"])
products_dataset = dir_file["products_dataset.csv"][["product_id", "product_category_name"]]
product_category_name_translation = dir_file["product_category_name_translation.csv"]

## merge and transform all data into datamart
### order_reviews_datase >< orders_dataset
datamart_mark = pd.merge(order_reviews_dataset, orders_dataset, on = "order_id", how = "left")

### merge with customers_dataset
datamart_mark = pd.merge(datamart_mark, customers_dataset, on = "customer_id", how = "left")

### merge the product_category_name_translation, order_items_dataset and products_dataset
product_merge = pd.merge(order_items_dataset, products_dataset, on = "product_id", how = "left")

product_merge = pd.merge(product_merge, product_category_name_translation, on = "product_category_name", how = "left")

product_merge[["product_category_name", "product_category_name_english"]] = product_merge[["product_category_name", "product_category_name_english"]].fillna("Name is not exist in database yet")

product_merge = product_merge.drop_duplicates(subset = ["order_id"], keep = "first")

### merge with product_merge
datamart_mark = pd.merge(datamart_mark, product_merge, on = "order_id", how = "left")

### restructure the datamart
datamart_mark = datamart_mark[["product_id", "product_category_name", "product_category_name_english", "review_id", "review_score",	"review_comment_title",	"review_comment_message", "review_answer_timestamp","order_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", 	"order_delivered_customer_date", "order_estimated_delivery_date", "customer_id", "customer_zip_code_prefix", "customer_city", "customer_state"]]

#%% Load

# create condition for creating new database and updating existing database
# this function will input all data in datamart into postgresql database system
def load(answer):
    if answer == "create":
        conn = psycopg2.connect(database="postgres",
                                user='postgres',
                                password='password', 
                                host='localhost',
                                port='5432')
        
        cursor = conn.cursor()
        
        create_db = """
        CREATE TABLE perqara_project (
            product_id char,
            product_category_name char,
            product_category_name_english char,
            review_id char,
            review_score integer,
            review_comment_title char,
            review_comment_message char,
            review_answer_timestamp timestamp,
            order_id char,
            order_status char,
            order_purchase_timestamp timestamp,
            order_approved_at timestamp,
            order_delivered_carried_date timestamp,
            order_delivered_customer_date timestamp,
            order_estimated_delivery_date timestamp,
            customer_id char,
            customer_zip_code_prefix integer,
            customer_city char,
            customer_state char
            )
        """
        cursor.execute(create_db)
        
        insert_data = "INSERT INTO perqara_project VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        for value in datamart_mark.values:
            x = list(value)
            cursor.execute(insert_data, (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18]))


        conn.commit()

        cursor.close()
        conn.close()
        
    if answer == "update":
        conn = psycopg2.connect(database="postgres",
                                user='postgres', password='password', 
                                host='localhost', port='5432')
        cursor = conn.cursor()
        
        insert_data = "INSERT INTO perqara_project VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        for value in datamart_mark.values:
            x = list(value)
            cursor.execute(insert_data, (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18]))
                           
        conn.commit()
            
        cursor.close()
        conn.close()
        
    else:
        print("""
              Please type 'create' into the 'load()' function to input the datamart 
              into a new table or type 'update' to input the datamart into an 
              existing table
              :)""")

        
load("create")

end_time_etl = datetime.now()


diff_time = end_time_etl - start_time_etl

print("""
      The program start at {}
      and end at {}.
      The program runs for {} seconds
      """.format(start_time_etl.strftime("%H:%M:%S"), end_time_etl.strftime("%H:%M:%S"), diff_time.seconds))
