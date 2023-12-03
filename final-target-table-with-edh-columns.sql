-- Create the Target Table table to store sales-related information from SalesFact Table and EDH Columns
CREATE OR REPLACE TABLE TargetTable (
    SeqNo INT IDENTITY(1,1) PRIMARY KEY
	
	-- Columns related to order details
    , OrderId INT
    , OrderDate DATE
    , OrderQty INT
    , UnitPrice DECIMAL(18,2)
    , UnitPriceDiscount DECIMAL(18,2)
    , ShipDate DATE
    , DueDate DATE
    , CustomerId INT
    , CustomerName VARCHAR()
	
	-- Columns related to product details
    , ProductId INT
    , ProductName VARCHAR()
    , ModelName VARCHAR()
    , MakeFlag BOOLEAN
    , StandardCost DECIMAL(18,2)
    , ListPrice DECIMAL(18,2)
    , CategoryName VARCHAR()
    , SubCategoryName VARCHAR()
	
	-- Columns related to vendor details
    , VendorID INT
    , VendorName VARCHAR()
    , VendorAccountNumber VARCHAR()
    , VendorCreditRating INT
    , VendorActiveFlag BOOLEAN
	
	-- Columns related to date and time dimensions
    , DayType VARCHAR()
    , Day VARCHAR()
    , DayShort VARCHAR()
    , Quarter VARCHAR()
    , QuarterYear VARCHAR()
    , Month INT
    , MonthName VARCHAR()
    , MonthYear VARCHAR()
    , Week INT
    , WeekName VARCHAR()
    , Year INT
	
	-- Additional columns related to delivery and shipment details
    , DeliveryTAT INT
    , ShipmentStatus VARCHAR()
    , ProductType VARCHAR()
	
	-- Financial columns
    , OrderValue DECIMAL(18,2)
    , TaxAmount DECIMAL(18,2)
    , FreightAmount DECIMAL(18,2)
    , ExtendedAmount DECIMAL(18,2)
    , SubTotal DECIMAL(18,2)
	
	-- Enterprise Data Hub (EDH) columns
    , EDH_ROW_HASH_NBR INT
    , EDH_DML_IND VARCHAR()
    , EDH_CREAT_TS DATETIME DEFAULT GETDATE()
    , EDH_UPDT_TS TIMESTAMP_NTZ -- DEFAULT GETDATE()
    , EDH_IS_C_FLG BOOLEAN
    , EDH_REPLICATION_SEQUENCE_NUMBER VARCHAR() DEFAULT '0'
    , EDH_LINEAGE_ID VARCHAR() DEFAULT '0'
    , EDH_MODIFIED_BY_USER_NAME NVARCHAR(255) DEFAULT current_user() 
);




/*
 * Creates or replaces a stored procedure named EDH_PROCEDURE().
 * This procedure manages data operations, transformations, and updates within the database.
 * It involves data validation, transformation, merging, and error handling processes to ensure data integrity and consistency.
 */
CREATE OR REPLACE PROCEDURE EDH_PROCEDURE()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
try {


	// No Transformation Section - Validates certain conditions in the date columns
    var noTransformSQL = `
        SELECT 
            CASE 
                WHEN OrderDate > CURRENT_DATE() THEN 'OrderDate cannot be in the future\n'
                ELSE ''
            END || 
            CASE 
                WHEN ShipDate < OrderDate THEN 'ShipDate cannot be before OrderDate\n'
                ELSE ''
            END || 
            CASE 
                WHEN DueDate < ShipDate THEN 'DueDate cannot be before ShipDate\n'
                ELSE ''
            END AS ValidationResults
        FROM 
            SalesFact`;
	
	// Executes SQL to check certain conditions in the data
    var stmtNoTransform = snowflake.createStatement({sqlText: noTransformSQL});
    var resultNoTransform = stmtNoTransform.execute();
    
	// Processes and formats validation results
    let validationResult = '';

    while (resultNoTransform.next()) {
        for (let i = 1; i <= resultNoTransform.getColumnCount(); i++) {
            validationResult += resultNoTransform.getColumnValue(i) + '\n'; // Fetch values directly
        }
        validationResult += '\n'; // Add extra newline for separation
    }



    // Date Transformation Section - Performs date-related transformations on the data
    var dateTransformSQL = `
        SELECT
            CASE 
                WHEN DAYOFWEEK(OrderDate) BETWEEN 2 AND 6 THEN 'Weekday' 
                ELSE 'Weekend' 
            END AS DayType
            , CASE 
                WHEN DAYNAME(OrderDate) = 'Sun' THEN 'Sunday'
                WHEN DAYNAME(OrderDate) = 'Mon' THEN 'Monday'
                WHEN DAYNAME(OrderDate) = 'Tue' THEN 'Tuesday'
                WHEN DAYNAME(OrderDate) = 'Wed' THEN 'Wednesday'
                WHEN DAYNAME(OrderDate) = 'Thu' THEN 'Thursday'
                WHEN DAYNAME(OrderDate) = 'Fri' THEN 'Friday'
                WHEN DAYNAME(OrderDate) = 'Sat' THEN 'Saturday'
            END AS Day
            , DAYNAME(OrderDate) AS DayShort
            , CASE 
                WHEN QUARTER(OrderDate) = 1 THEN 'Q1'
                WHEN QUARTER(OrderDate) = 2 THEN 'Q2'
                WHEN QUARTER(OrderDate) = 3 THEN 'Q3'
                WHEN QUARTER(OrderDate) = 4 THEN 'Q4'
            END AS Quarter
            , CONCAT(YEAR(OrderDate), '-Q'
                , CASE 
                    WHEN QUARTER(OrderDate) = 1 THEN '1'
                    WHEN QUARTER(OrderDate) = 2 THEN '2'
                    WHEN QUARTER(OrderDate) = 3 THEN '3'
                    WHEN QUARTER(OrderDate) = 4 THEN '4'
                END) AS QuarterYear
            , MONTH(OrderDate) AS Month
            , CONCAT(LEFT(MONTHNAME(OrderDate), 3), ' ', YEAR(OrderDate)) AS MonthName
            , CONCAT(YEAR(OrderDate), '-', LPAD(MONTH(OrderDate), 2, '0')) AS MonthYear
            , CEIL(DAY(OrderDate) / 7) AS Week
            , CONCAT('Wk-', CEIL(DAY(OrderDate) / 7)) AS WeekName
            , YEAR(OrderDate) AS Year
        FROM 
            SalesFact`;
	
	
	
	// Shipment Details Section - Processes shipment-related details
    var stmtDateTransform = snowflake.createStatement({ sqlText: dateTransformSQL });
    stmtDateTransform.execute();

    // Shipment Details Section
    var shipmentDetailsSQL = `
        SELECT
            DATEDIFF(DAY, OrderDate, ShipDate) AS DeliveryTAT
            , CASE 
                WHEN ShipDate < DueDate THEN 'Early'
                WHEN ShipDate = DueDate THEN 'Normal'
                ELSE 'Late'
            END AS ShipmentStatus
            , CASE 
                WHEN MakeFlag = 1 THEN 'Manufactured'
                WHEN MakeFlag = 0 THEN 'Purchased'
                ELSE 'Unknown'
            END AS ProductType
        FROM 
            SalesFact`;

	// Executes SQL for shipment-related details
    var stmtShipmentDetails = snowflake.createStatement({ sqlText: shipmentDetailsSQL });
    stmtShipmentDetails.execute();



    // Calculated Order Details Section - Computes and summarizes order-related details
    var calculatedOrderDetailsSQL = `
        WITH Transformations AS (
            SELECT
                o.SalesOrderID AS OrderId
                , o.OrderQty * (o.UnitPrice - (o.UnitPrice * o.UnitPriceDiscount)) AS OrderValue
                , o.taxamt / NULLIFZERO(COUNT(o.productid) OVER (PARTITION BY o.SalesOrderID)) AS TaxAmount
                , o.freight / NULLIFZERO(COUNT(o.productid) OVER (PARTITION BY o.SalesOrderID)) AS FreightAmount
                , (o.OrderQty * (o.UnitPrice - (o.UnitPrice * o.UnitPriceDiscount))) +
                    (o.taxamt / NULLIFZERO(COUNT(o.productid) OVER (PARTITION BY o.SalesOrderID))) +
                    (o.freight / NULLIFZERO(COUNT(o.productid) OVER (PARTITION BY o.SalesOrderID))) AS ExtendedAmount
            FROM
                Orders o
			JOIN SalesFact sf
				ON o.salesorderid = sf.orderid AND o.ProductId = sf.PRODUCTID
        ) 
        
        SELECT
            *
            , SUM(ExtendedAmount) OVER (PARTITION BY OrderId ORDER BY OrderId) AS SubTotal
        FROM
            Transformations`;

	// Executes SQL to compute order-related details
    var stmtCalculatedOrderDetails = snowflake.createStatement({ sqlText: calculatedOrderDetailsSQL });
    stmtCalculatedOrderDetails.execute();



    //EDH Columns Section
    var EDH_ROW_HASH_NBRSQL = `
                UPDATE SalesFact
                    SET EDH_ROW_HASH_NBR = hash(
                        MD5(
							// Concatenates and hashes multiple columns from SalesFact Table
                            ORDERID || '~' ||
                            TO_CHAR(ORDERDATE, 'YYYYMMDD') || '~' ||
                            TO_CHAR(ORDERQTY) || '~' ||
                            TO_CHAR(UNITPRICE) || '~' ||
                            TO_CHAR(UNITPRICEDISCOUNT) || '~' ||
                            TO_CHAR(SHIPDATE, 'YYYYMMDD') || '~' ||
                            TO_CHAR(DUEDATE, 'YYYYMMDD') || '~' ||
                            TO_CHAR(CUSTOMERID) || '~' ||
                            CUSTOMERNAME || '~' ||
                            TO_CHAR(PRODUCTID) || '~' ||
                            PRODUCTNAME || '~' ||
                            MODELNAME || '~' ||
                            TO_CHAR(MAKEFLAG) || '~' ||
                            TO_CHAR(STANDARDCOST) || '~' ||
                            TO_CHAR(LISTPRICE) || '~' ||
                            CATEGORYNAME || '~' ||
                            SUBCATEGORYNAME || '~' ||
                            TO_CHAR(VENDORID) || '~' ||
                            VENDORNAME || '~' ||
                            VENDORACCOUNTNUMBER || '~' ||
                            TO_CHAR(VENDORCREDITRATING) || '~' ||
                            TO_CHAR(VENDORACTIVEFLAG) || '~' ||
                            DAYTYPE || '~' ||
                            DAY || '~' ||
                            DAYSHORT || '~' ||
                            TO_CHAR(QUARTER) || '~' ||
                            TO_CHAR(QUARTERYEAR) || '~' ||
                            TO_CHAR(MONTH) || '~' ||
                            MONTHNAME || '~' ||
                            TO_CHAR(MONTHYEAR) || '~' ||
                            TO_CHAR(WEEK) || '~' ||
                            WEEKNAME || '~' ||
                            TO_CHAR(YEAR) || '~' ||
                            DELIVERYTAT || '~' ||
                            SHIPMENTSTATUS || '~' ||
                            PRODUCTTYPE || '~' ||
                            TO_CHAR(ORDERVALUE) || '~' ||
                            TO_CHAR(TAXAMOUNT) || '~' ||
                            TO_CHAR(FREIGHTAMOUNT) || '~' ||
                            TO_CHAR(EXTENDEDAMOUNT) || '~' ||
                            TO_CHAR(SUBTOTAL)
                        )
                    );
                `;

	// Executes SQL to update EDH details
    var stmtEDH_ROW_HASH_NBR = snowflake.createStatement({sqlText: EDH_ROW_HASH_NBRSQL});
    stmtEDH_ROW_HASH_NBR.execute();

 
    // Merging into TargetTable
    var merge_command = `
        MERGE INTO TargetTable AS Target
        USING (
			// Selects data from SalesFact for merging
            SELECT
                SeqNo
				
                // Columns from the No Transformation Section
                , OrderId, OrderDate, OrderQty, UnitPrice, UnitPriceDiscount
                , ShipDate, DueDate, CustomerId, CustomerName
                , ProductId, ProductName, ModelName, MakeFlag, StandardCost, ListPrice
                , CategoryName, SubCategoryName, VendorID, VendorName
                , VendorAccountNumber, VendorCreditRating, VendorActiveFlag
				
                // Columns from the Date Transformation Section
                , DayType, Day, DayShort
				, Quarter, QuarterYear
				, Month, MonthName, MonthYear
				, Week, WeekName
                , Year
				
                // Columns from the Shipment Details Section
                , DeliveryTAT
                , ShipmentStatus
                , ProductType
				
                // Columns from the Calculated Order Details Section
                , OrderValue
                , TaxAmount
                , FreightAmount
                , ExtendedAmount
                , SubTotal

                // Columns from the EDH Section
                , EDH_ROW_HASH_NBR
                , EDH_DML_IND
                , EDH_CREAT_TS
                , EDH_UPDT_TS
                , EDH_IS_C_FLG
                , EDH_REPLICATION_SEQUENCE_NUMBER
                , EDH_LINEAGE_ID
                , EDH_MODIFIED_BY_USER_NAME
				
            FROM SalesFact
            			
            // Additional JOINs or WHERE conditions if needed
			
        ) AS Source
        ON Target.SeqNo = Source.SeqNo
        WHEN MATCHED THEN
            UPDATE SET
				// Updates existing records if matched
                SeqNo = Source.SeqNo
                , OrderId = Source.OrderId
                , OrderDate = Source.OrderDate
                , OrderQty = Source.OrderQty
                , UnitPrice = Source.UnitPrice
                , UnitPriceDiscount = Source.UnitPriceDiscount
                , ShipDate = Source.ShipDate
                , DueDate = Source.DueDate
                , CustomerId = Source.CustomerId
                , CustomerName = Source.CustomerName
                , ProductId = Source.ProductId
                , ProductName = Source.ProductName
                , ModelName = Source.ModelName
                , MakeFlag = Source.MakeFlag
                , StandardCost = Source.StandardCost
                , ListPrice = Source.ListPrice
                , CategoryName = Source.CategoryName
                , SubCategoryName = Source.SubCategoryName
                , VendorID = Source.VendorID
                , VendorName = Source.VendorName
                , VendorAccountNumber = Source.VendorAccountNumber
                , VendorCreditRating = Source.VendorCreditRating
                , VendorActiveFlag = Source.VendorActiveFlag
				
				, DayType = Source.DayType
                , Day = Source.Day
                , DayShort = Source.DayShort
                , Quarter = Source.Quarter
                , QuarterYear = Source.QuarterYear
                , Month = Source.Month
                , MonthName = Source.MonthName
                , MonthYear = Source.MonthYear
                , Week = Source.Week
                , WeekName = Source.WeekName
                , Year = Source.Year
				
				, DeliveryTAT = Source.DeliveryTAT
                , ShipmentStatus = Source.ShipmentStatus
                , ProductType = Source.ProductType
				
				, OrderValue = Source.OrderValue
                , TaxAmount = Source.TaxAmount
                , FreightAmount = Source.FreightAmount
                , ExtendedAmount = Source.ExtendedAmount
                , SubTotal = Source.SubTotal
                
                , EDH_ROW_HASH_NBR = Source.EDH_ROW_HASH_NBR
                , EDH_DML_IND = 'U'
                , EDH_CREAT_TS = Source.EDH_CREAT_TS
                , EDH_UPDT_TS = current_date()
                , EDH_IS_C_FLG = 1
                , EDH_REPLICATION_SEQUENCE_NUMBER = Source.EDH_REPLICATION_SEQUENCE_NUMBER
                , EDH_LINEAGE_ID = Source.EDH_LINEAGE_ID
                , EDH_MODIFIED_BY_USER_NAME = Source.EDH_MODIFIED_BY_USER_NAME
				
				
        WHEN NOT MATCHED THEN
            INSERT (
				// Inserts new records if not matched
                SeqNo, OrderId, OrderDate, OrderQty, UnitPrice, UnitPriceDiscount
                , ShipDate, DueDate, CustomerId, CustomerName
                , ProductId, ProductName, ModelName, MakeFlag, StandardCost, ListPrice, CategoryName
                , SubCategoryName, VendorID, VendorName, VendorAccountNumber, VendorCreditRating, VendorActiveFlag
				, DayType, Day, DayShort, Quarter, QuarterYear, Month, MonthName, MonthYear, Week, WeekName, Year
				, DeliveryTAT, ShipmentStatus, ProductType
				, OrderValue, TaxAmount, FreightAmount, ExtendedAmount, SubTotal
                , EDH_ROW_HASH_NBR,EDH_DML_IND, EDH_CREAT_TS,EDH_UPDT_TS, EDH_IS_C_FLG, EDH_REPLICATION_SEQUENCE_NUMBER, EDH_LINEAGE_ID, EDH_MODIFIED_BY_USER_NAME
                )
            VALUES (
				// Values for new records
                Source.SeqNo, Source.OrderId, Source.OrderDate, Source.OrderQty, Source.UnitPrice
                , Source.UnitPriceDiscount, Source.ShipDate, Source.DueDate, Source.CustomerId, Source.CustomerName
                , Source.ProductId, Source.ProductName, Source.ModelName, Source.MakeFlag, Source.StandardCost, Source.ListPrice
                , Source.CategoryName, Source.SubCategoryName, Source.VendorID, Source.VendorName
                , Source.VendorAccountNumber, Source.VendorCreditRating, Source.VendorActiveFlag
				, Source.DayType, Source.Day, Source.DayShort, Source.Quarter, Source.QuarterYear, Source.Month, Source.MonthName, Source.MonthYear, Source.Week, Source.WeekName, Source.Year
				, Source.DeliveryTAT, Source.ShipmentStatus, Source.ProductType
				, Source.OrderValue, Source.TaxAmount, Source.FreightAmount, Source.ExtendedAmount, Source.SubTotal
                , Source.EDH_ROW_HASH_NBR, 'I', Source.EDH_CREAT_TS, EDH_UPDT_TS, 0, Source.EDH_REPLICATION_SEQUENCE_NUMBER, Source.EDH_LINEAGE_ID, Source.EDH_MODIFIED_BY_USER_NAME
                );
    `;

	// Executes SQL for merging data into the TargetTable
    var mergeStmt = snowflake.createStatement({ sqlText: merge_command });
    mergeStmt.execute();

	// Returns success message if all procedures executed successfully
    return 'All procedures executed and data merged into Target Table.';
} catch (err) {
	// Handles different error codes and generates specific error messages
    let errorMessage;

    switch (err.code) {
        case 100017:
            errorMessage = 'Invalid Date Format: ' + err.message;
            break;
        case 100018:
            errorMessage = 'Missing or Incomplete Dates: ' + err.message;
            break;
        case 100:
            errorMessage = 'Data Type Mismatch Error: ' + err.message;
            break;
        case 99999:
            errorMessage = 'Unexpected Error: ' + err.message;
            break;
        case 1003:
            errorMessage = 'Object Not Found Error: ' + err.message;
            break;
        case 1004:
            errorMessage = 'Feature Not Supported Error: ' + err.message;
            break;
        case 1005:
            errorMessage = 'Resource Limit Exceeded Error: ' + err.message;
            break;
        case 10001:
            errorMessage = 'Division by Zero Error: Check for potential division by zero scenarios in your calculations.';
            break;
        case '1001':
            errorMessage = 'Error: Duplicate entry. Data already exists.';
            break;
        default:
            errorMessage = 'An error occurred: ' + err.message;
            break;
    }

    return errorMessage;
} finally {
	// Cleanup operations if required
    // Ensure the file is closed whether an error occurred or not - Resource Cleanup
    // Ensure the database connection is closed - Cleanup After Database Operations
    // Regardless of success or failure, clear the temporary data - Clearing Temporary Data
}

$$;

-- Executes the stored procedure
CALL EDH_PROCEDURE();

-- Retrieves data from the updated TargetTable
SELECT * FROM TargetTable;
