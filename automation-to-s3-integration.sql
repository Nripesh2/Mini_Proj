-- AWS CLI Commands

-- AWS Access Key ID [None]: AKIAXQT3GSK6UP4YDZTD
-- AWS Secret Access Key [None]: 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
-- Default region name [None]: ap-south-1
-- Default output format [None]: json

-- cd 'C:\Users\Rakshita\Downloads\Mini Project-Atgeir Solutions'
-- python local-machine-to-s3-automation.py


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

--Creation of external stage
CREATE OR REPLACE STAGE csv_external_stage
STORAGE_INTEGRATION = S3_LOCAL_INTEGRATION
URL = 's3://salescsvupload/data/'
FILE_FORMAT = (TYPE = CSV);

--Creating a procedure to automate copy commands from aws bucket to snowflake
CREATE OR REPLACE PROCEDURE execute_csv_import()
	RETURNS STRING
	LANGUAGE JAVASCRIPT
	EXECUTE AS CALLER
	AS
	$$

		// SQL script for creating the file format
		var sql_command1 = `
		CREATE OR REPLACE FILE FORMAT csv_format
		TYPE = 'CSV'
		FIELD_DELIMITER = ','
		COMPRESSION = NONE
		SKIP_HEADER = 1;
		`;
		// Execute the SQL command
		var statement1 = snowflake.createStatement({sqlText: sql_command1});
		statement1.execute();

		var sql_command2 = `
		CREATE OR REPLACE FILE FORMAT comma_csv_format
		TYPE = 'CSV'
		FIELD_OPTIONALLY_ENCLOSED_BY = '"'
		FIELD_DELIMITER = ','
		COMPRESSION = NONE
		SKIP_HEADER = 1;
		`;

		// Execute the SQL command
		var statement2 = snowflake.createStatement({sqlText: sql_command2});
		statement2.execute();

		var trunctate_command1 = `
		TRUNCATE TABLE CUSTOMERS;
		`;
		// Execute the SQL command
		var st1 = snowflake.createStatement({sqlText: trunctate_command1});
		st1.execute();

		var sql_command3 = `
		COPY INTO customers
		FROM @csv_external_stage
		FILES = ('customers.csv')
		FILE_FORMAT = (FORMAT_NAME = 'csv_format');
		`;
		// Execute the SQL command
		var statement3 = snowflake.createStatement({sqlText: sql_command3});
		statement3.execute();


		var trunctate_command2 = `
		TRUNCATE TABLE orders;
		`;
		// Execute the SQL command
		var st2 = snowflake.createStatement({sqlText: trunctate_command2});
		st2.execute();

		var sql_command4 = `
		COPY INTO orders
		FROM @csv_external_stage
		FILES = ('orders.csv')
		FILE_FORMAT = (FORMAT_NAME = 'csv_format');
		`;
		// Execute the SQL command
		var statement4 = snowflake.createStatement({sqlText: sql_command4});
		statement4.execute();


		var trunctate_command3 = `
		TRUNCATE TABLE employees;
		`;
		// Execute the SQL command
		var st3 = snowflake.createStatement({sqlText: trunctate_command3});
		st3.execute();

		var sql_command5 = `
		COPY INTO employees
		FROM @csv_external_stage
		FILES = ('employees.csv')
		FILE_FORMAT = (FORMAT_NAME = 'csv_format');
		`;
		// Execute the SQL command
		var statement5 = snowflake.createStatement({sqlText: sql_command5});
		statement5.execute();


		var trunctate_command4 = `
		TRUNCATE TABLE productcategories;
		`;
		// Execute the SQL command
		var st4 = snowflake.createStatement({sqlText: trunctate_command4});
		st4.execute();

		var sql_command6 = `
		COPY INTO productcategories
		FROM @csv_external_stage
		FILES = ('productcategories.csv')
		FILE_FORMAT = (FORMAT_NAME = 'csv_format');
		`;
		// Execute the SQL command
		var statement6 = snowflake.createStatement({sqlText: sql_command6});
		statement6.execute();


		var trunctate_command5 = `
		TRUNCATE TABLE products;
		`;
		// Execute the SQL command
		var st5 = snowflake.createStatement({sqlText: trunctate_command5});
		st5.execute();

		var sql_command7 = `
		COPY INTO products
		FROM @csv_external_stage
		FILES = ('products.csv')
		FILE_FORMAT = (FORMAT_NAME = 'comma_csv_format');
		`;
		// Execute the SQL command
		var statement7 = snowflake.createStatement({sqlText: sql_command7});
		statement7.execute();


		var trunctate_command6 = `
		TRUNCATE TABLE productsubcategories;
		`;
		// Execute the SQL command
		var st6 = snowflake.createStatement({sqlText: trunctate_command6});
		st6.execute();

		var sql_command8 = `
		COPY INTO productsubcategories
		FROM @csv_external_stage
		FILES = ('productsubcategories.csv')
		FILE_FORMAT = (FORMAT_NAME = 'csv_format');
		`;
		// Execute the SQL command
		var statement8 = snowflake.createStatement({sqlText: sql_command8});
		statement8.execute();


		var trunctate_command7 = `
		TRUNCATE TABLE vendorproduct;
		`;
		// Execute the SQL command
		var st7 = snowflake.createStatement({sqlText: trunctate_command7});
		st7.execute();

		var sql_command9 = `
		COPY INTO vendorproduct
		FROM @csv_external_stage
		FILES = ('vendorproduct.csv')
		FILE_FORMAT = (FORMAT_NAME = 'csv_format');
		`;
		// Execute the SQL command
		var statement9 = snowflake.createStatement({sqlText: sql_command9});
		statement9.execute();


		var trunctate_command8 = `
		TRUNCATE TABLE vendors;
		`;
		// Execute the SQL command
		var st8 = snowflake.createStatement({sqlText: trunctate_command8});
		st8.execute();

		var sql_command10 = `
		COPY INTO vendors
		FROM @csv_external_stage
		FILES = ('vendors.csv')
		FILE_FORMAT = (FORMAT_NAME = 'comma_csv_format');
		`;
		// Execute the SQL command
		var statement10 = snowflake.createStatement({sqlText: sql_command10});
		statement10.execute();


		var trunctate_command9 = `
		TRUNCATE TABLE ordersmaster;
		`;
		// Execute the SQL command
		var st9 = snowflake.createStatement({sqlText: trunctate_command9});
		st9.execute();

		var sql_command11 = `
		COPY INTO ordersmaster
		FROM @csv_external_stage
		FILES = ('orders_master.csv')
		FILE_FORMAT = (FORMAT_NAME = 'csv_format');
		`;
		// Execute the SQL command
		var statement11 = snowflake.createStatement({sqlText: sql_command11});
		statement11.execute();

		return 'Procedure created successfully';
	$$;

--Calling the stored procedure
CALL execute_csv_import();

-- Creating a task scheduled to automatically load files from s3 bucket to Snowflake
-- CREATE OR REPLACE TASK csv_import_task
-- WAREHOUSE = COMPUTE_WH
-- SCHEDULE = '60 MINUTES'
-- AS
-- CALL execute_csv_import();

-- ALTER TASK csv_import_task RESUME;

-- show tasks;