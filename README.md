#adf repo - This Repo consits of ADF Assignments


**1. Creating Container in ADLS**

Aim: To create a container named sales_view_devst in Azure Data Lake Storage and organize folders for customer, product, store, and sales.

Steps:

Create a Storage Account:
Go to the Azure Portal.
Navigate to "Storage accounts" and click on "Add".
Choose subscription, resource group, storage account name, location, and performance options.
Click on "Review + create" and then "Create".
Create Container:
After the storage account is created, navigate to it.
Under "Blob service" section, select "Containers".
Click on "+ Container" to create a new container.
Name the container as "sales_view_devst" and set access level.
Click "Create" to create the container.
Create Folders and Upload Files:
Inside the container "sales_view_devst", create folders for "customer", "product", "store", and "sales".
Upload files into their respective folders to make it work in real-time.

**2. Creating ADF Pipeline**

Aim: To create an ADF pipeline to dynamically retrieve the latest modified files from ADLS folders.

Steps:

1.Pipeline Setup:
Create a pipeline in Azure Data Factory.
2.Metadata Activity:
Add a "Get Metadata" activity to the pipeline to retrieve metadata from ADLS folders.
3.Dataset Creation:
Create a new dataset in metadata activity settings allowing dynamic content.
4.Dynamic Content:
Add parameters to the dataset for dynamic content.
5.Pipeline Configuration:
Specify "child items" as field list and add dynamic content under dataset section.
6.For Each Activity:
Include a "For Each" activity in the pipeline to iterate through each file dynamically.
7.Activities Configuration:
Add activities within the "For Each" loop, such as another "Get Metadata" activity.
8.Parameterization:
Parameterize the dataset and include dynamic content in connection settings.
9.Pipeline Execution:
Run the pipeline to execute defined activities.

**3. Bronze Layer Creation**

Aim: To create a bronze layer with subfolders for customer, product, store, and sales.

Steps:

Create Container:
Create a container named "bronze".
Copy Data Activity:
In the pipeline, search for copy data activity.
Provide required details in source and sink and add parameters for dynamic content.
Debug it multiple times for each folder sequentially.
Verification:
Check the bronze container to see all folders and the last modified file under each folder.

**4. Customer Data Transformation**

Aim: To transform customer data according to specific requirements.

Steps:

Column Header Formatting:
Convert all column headers to snake case using a UDF function.
Split Name Column:
Split the "Name" column into "first_name" and "last_name".
Extract Domain from Email:
Create a column "domain" by extracting from the "email" column.
Gender Mapping:
Map gender values to "M" and "F" based on existing values.
Date Column Manipulation:
Split "Joining date" into "date" and "time" columns.
Format the "date" column to "yyyy-MM-dd".
Expenditure Status:
Create "expenditure-status" column based on the "spent" column.
Data Writing:
Write data based on upsert into customer table in the silver layer.

**5. Product Data Transformation**

Aim: To transform product data according to specific requirements.

Steps:

Column Header Formatting:
Convert all column headers to snake case using a UDF function.
Create Sub-Category:
Create "sub_category" column based on "category" column.
Data Writing:
Write data based on upsert into product table in the silver layer.

**6. Store Data Transformation**

Aim: To transform store data according to specific requirements.

Steps:

Column Header Formatting:
Convert all column headers to snake case using a UDF function.
Create Store Category:
Extract store category from email and create "store_category" column.
Date Formatting:
Format "created_at" and "updated_at" columns to "yyyy-MM-dd".
Data Writing:
Write data based on upsert into store table in the silver layer.

**7. Sales Data Transformation**

Aim: To transform sales data according to specific requirements.

Steps:

Column Header Formatting:
Convert all column headers to snake case using a UDF function.
Data Writing:
Write data based on upsert into customer_sales table in the silver layer.

**8. Gold Layer Processing**

Aim: To process data for the gold layer.

Steps:

Data Retrieval:
Retrieve required data using product and store tables.
Data Transformation:
Transform data based on specific requirements.
Data Writing:
Write data based on overwrite into StoreProductSalesAnalysis table in the gold layer.

**9. Data Profiling**

Aim: To perform data profiling for the provided data.

Steps:

Summary Statistics:
Compute summary statistics for numerical and categorical columns.
Data Quality Checks:
Check for missing values, outliers, and anomalies.
Data Distribution:
Examine the distribution of values within each column.
Data Relationships:
Explore relationships between variables through correlation analysis and cross-tabulations.
Data Profiling Reports:
Generate comprehensive data profiling reports summarizing the findings.
