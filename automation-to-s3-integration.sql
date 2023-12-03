-- AWS CLI Commands

-- AWS Access Key ID [None]: AKIAXQT3GSK6UP4YDZTD
-- AWS Secret Access Key [None]: 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
-- Default region name [None]: ap-south-1
-- Default output format [None]: json

-- cd 'C:\Users\Rakshita\Downloads\Mini Project-Atgeir Solutions'
-- python aws_s3_csv_upload.py


-- SNOWFLAKE WORKSHEET

-- Use the specified Snowflake warehouse for computations.
use warehouse COMPUTE_WH;


-- Create a new database named MINI_PROJECT_DB and switch to it.
create database MINI_PROJECT_DB;
use database MINI_PROJECT_DB;


-- Create or replace a storage integration named S3_LOCAL_INTEGRATION for S3 external stage.
-- Configure the integration to access the specified AWS S3 bucket and define the role ARN.
CREATE OR REPLACE STORAGE INTEGRATION S3_LOCAL_INTEGRATION
TYPE = EXTERNAL_STAGE
ENABLED = true
STORAGE_PROVIDER = S3
storage_aws_role_arn='arn:aws:iam::516728197821:role/snowflake_integration'
STORAGE_ALLOWED_LOCATIONS = ('s3://salescsvupload/data/');


-- Describe the properties and details of the S3 storage integration.
DESC integration S3_LOCAL_INTEGRATION;


-- Create or replace an external stage named csv_external_stage.
-- Associate it with the S3 storage integration for file storage and retrieval.
-- Define the URL path in the S3 bucket and set the file format as CSV.
CREATE OR REPLACE STAGE csv_external_stage
STORAGE_INTEGRATION = S3_LOCAL_INTEGRATION
URL = 's3://salescsvupload/data/'
FILE_FORMAT = (TYPE = CSV);


-- List the contents of the csv_external_stage for verification.
list @csv_external_stage;


-- Define file formats csv_format and comma_csv_format for CSV data files.
-- Specify the settings for parsing CSV files including delimiter, header handling, and compression.
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
COMPRESSION = NONE
SKIP_HEADER = 1;


CREATE OR REPLACE FILE FORMAT comma_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FIELD_DELIMITER = ','
COMPRESSION = NONE
SKIP_HEADER = 1;


-- Copy data from CSV files in the S3 bucket to Snowflake tables:

-- 1. Customers table
COPY INTO customers
FROM @csv_external_stage
FILES = ('customers.csv')
FILE_FORMAT = (FORMAT_NAME = 'csv_format');


-- 2. Orders table
COPY INTO orders
FROM @csv_external_stage
FILES = ('orders.csv')
FILE_FORMAT = (FORMAT_NAME = 'csv_format');


-- 3. Employees table
COPY INTO employees
FROM @csv_external_stage
FILES = ('employees.csv')
FILE_FORMAT = (FORMAT_NAME = 'csv_format');


-- 4. Product Categories table
COPY INTO productcategories
FROM @csv_external_stage
FILES = ('productcategories.csv')
FILE_FORMAT = (FORMAT_NAME = 'csv_format');


-- 5. Products table
COPY INTO products
FROM @csv_external_stage
FILES = ('products.csv')
FILE_FORMAT = (FORMAT_NAME = 'comma_csv_format');


-- 5. Product Subcategories table
COPY INTO productsubcategories
FROM @csv_external_stage
FILES = ('productsubcategories.csv')
FILE_FORMAT = (FORMAT_NAME = 'csv_format');


-- 6. Vendor Product table
COPY INTO vendorproduct
FROM @csv_external_stage
FILES = ('vendorproduct.csv')
FILE_FORMAT = (FORMAT_NAME = 'csv_format');


-- 7. Vendors table
COPY INTO vendors
FROM @csv_external_stage
FILES = ('vendors.csv')
FILE_FORMAT = (FORMAT_NAME = 'comma_csv_format');


-- 8. Orders Master table
COPY INTO ordersmaster
FROM @csv_external_stage
FILES = ('orders_master.csv')
FILE_FORMAT = (FORMAT_NAME = 'csv_format');


-- Perform a SELECT operation to view the data from all tables.
select * from table_name;


-- The code successfully loads data from the specified CSV files in the S3 bucket into corresponding Snowflake tables.