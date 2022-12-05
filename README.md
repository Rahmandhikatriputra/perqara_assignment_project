# perqara_assignment_project
This is an assignment for a data engineer position at Perqara.
This Python file helps the user (especially for the marketing team) to create a datamart.

The datamart consists:
> 1. order_reviews_dataset:            Informing the review of a product based on order
> 2. orders_dataset:                   Informing the historical and status of order process
> 3. order_item_dataset:               Informing what are the products on each order
> 4. customers_dataset:                Customer demographic information
> 5. products_dataset:                 Detailed information on each product
> 6. product_category_name_traslation: Translation of the product name to englsih

This Python file consists of 3 main parts:
> 1. Extract:    All data sources will be imported and read into Python program
> 2. Transform:  Performing all sort of tranform and manipulation processes to normalize the dataset. 
              This processes include changing data type into a suitable type, changing the null values
              into a appropriate value if it is possible, identify any duplicated rows, etc. 
              The end of this process is to create a datamart for the marketing division.
> 3. Load:       The Load process for this Python program is using PostgreSQL as the system database 
              to store the datamart. On the Load process, user can choose either "create" or "update"
              the datamart. The "create" command lets the program to create a new table insert the datamart
              into the new table, while the "update" command will update an existing table with the 
              new datamart dataset
              
Thank you for reading!!
If there is any critic and feedback for this program, please e-mail me at : rahmandikatriputra@gmail.com
or simply reach me through Linkedin: https://www.linkedin.com/in/rahmandika-tri-putra/

Warm regards,
Dhika
