-- Create the SalesFact table to store sales-related information from multiple tables
CREATE OR REPLACE TABLE SalesFact (
    SeqNo INT IDENTITY(1,1) PRIMARY KEY, -- Sequential identifier for the sales facts
	
    -- Columns related to order details
    , OrderId INT -- Unique identifier for the order
    , OrderDate DATE -- Date of the order
    , OrderQty INT -- Quantity of products ordered
    , UnitPrice DECIMAL(18,2) -- Price per unit of the product
    , UnitPriceDiscount DECIMAL(18,2) -- Discount on unit price
    , ShipDate DATE -- Date of shipment
    , DueDate DATE -- Due date for the order
    , CustomerId INT -- Identifier for the customer
    , CustomerName VARCHAR() -- Name of the customer
	
    -- Columns related to product details
    , ProductId INT -- Identifier for the product
    , ProductName VARCHAR() -- Name of the product
    , ModelName VARCHAR() -- Model of the product
    , MakeFlag BOOLEAN -- Indicates if the product is manufactured
    , StandardCost DECIMAL(18,2) -- Standard cost of the product
    , ListPrice DECIMAL(18,2) -- List price of the product
    , CategoryName VARCHAR() -- Name of the product category
    , SubCategoryName VARCHAR() -- Name of the product sub-category
	
    -- Columns related to vendor details
    , VendorID INT -- Identifier for the vendor
    , VendorName VARCHAR() -- Name of the vendor
    , VendorAccountNumber VARCHAR() -- Account number of the vendor
    , VendorCreditRating INT -- Credit rating of the vendor
    , VendorActiveFlag BOOLEAN -- Indicates if the vendor is active
	
    -- Columns related to date and time dimensions
    , DayType VARCHAR() -- Type of day (Weekday/Weekend)
    , Day VARCHAR() -- Day of the week
    , DayShort VARCHAR() -- Short representation of the day
    , Quarter VARCHAR() -- Quarter of the year
    , QuarterYear VARCHAR() -- Year and quarter combination
    , Month INT -- Month number
    , MonthName VARCHAR() -- Name of the month
    , MonthYear VARCHAR() -- Year and month combination
    , Week INT -- Week number
    , WeekName VARCHAR() -- Name of the week
    , Year INT -- Year
	
    -- Additional columns related to delivery and shipment details
    , DeliveryTAT INT -- Delivery Turnaround Time
    , ShipmentStatus VARCHAR() -- Status of the shipment (Early/Normal/Late)
    , ProductType VARCHAR() -- Type of the product (Manufactured/Purchased)
	
    -- Financial columns
    , OrderValue DECIMAL(18,2) -- Total value of the order
    , TaxAmount DECIMAL(18,2) -- Tax amount
    , FreightAmount DECIMAL(18,2) -- Freight amount
    , ExtendedAmount DECIMAL(18,2) -- Extended amount including tax and freight
    , SubTotal DECIMAL(18,2) -- Subtotal amount
	
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


-- Insert data into the SalesFact table with transformations
INSERT INTO SalesFact (
    OrderId, OrderDate, OrderQty, UnitPrice, UnitPriceDiscount
    , ShipDate, DueDate, CustomerId, CustomerName
    , ProductId, ProductName, ModelName, MakeFlag, StandardCost, ListPrice
    , CategoryName, SubCategoryName, VendorID, VendorName
    , VendorAccountNumber, VendorCreditRating, VendorActiveFlag
    , DayType, Day, DayShort, Quarter, QuarterYear, Month, MonthYear, MonthName, WEEK, WeekName, Year
    , DeliveryTAT, ShipmentStatus, ProductType, OrderValue, TaxAmount
    , FreightAmount, ExtendedAmount, SubTotal
)
-- Create CTE (Common Table Expression) to perform transformations on data from multiple tables
WITH Transformations AS (
	-- Select and transform data from various joined tables
    SELECT
        o.SalesOrderID AS OrderId
        , o.OrderDate
        , o.OrderQty
        , o.UnitPrice
        , o.UnitPriceDiscount
        , o.ShipDate
        , o.DueDate
        , c.CustomerId AS CustomerId
        , c.FullName AS CustomerName
        , p.ProductId
        , p.ProductName
        , p.ModelName
        , p.MakeFlag
        , p.StandardCost
        , p.ListPrice
        , pc.CategoryName
        , psc.SubCategoryName
        , vp.VendorID
        , v.VendorName
        , v.AccountNumber AS VendorAccountNumber
        , v.CreditRating AS VendorCreditRating
        , v.ActiveFlag AS VendorActiveFlag
        
        --DAYTYPE
        , CASE WHEN DAYOFWEEK(o.OrderDate) IN (2, 3, 4, 5, 6) THEN 'Weekday' ELSE 'Weekend' END AS DayType
        
        --DAY
        , CASE 
            WHEN DAYNAME(OrderDate) = 'Sun' THEN 'Sunday'
            WHEN DAYNAME(OrderDate) = 'Mon' THEN 'Monday'
            WHEN DAYNAME(OrderDate) = 'Tue' THEN 'Tuesday'
            WHEN DAYNAME(OrderDate) = 'Wed' THEN 'Wednesday'
            WHEN DAYNAME(OrderDate) = 'Thu' THEN 'Thursday'
            WHEN DAYNAME(OrderDate) = 'Fri' THEN 'Friday'
            WHEN DAYNAME(OrderDate) = 'Sat' THEN 'Saturday'
        END AS Day
        
        --DAYSHORT
        , DAYNAME(Orderdate) AS DayShort

        --QUATER
        , CASE 
            WHEN QUARTER(OrderDate) = 1 THEN 'Q1'
            WHEN QUARTER(OrderDate) = 2 THEN 'Q2'
            WHEN QUARTER(OrderDate) = 3 THEN 'Q3'
            WHEN QUARTER(OrderDate) = 4 THEN 'Q4'
        END AS Quarter
        
        --QUATERYEAR
        , CONCAT(YEAR(OrderDate), '-Q',
            CASE 
                WHEN QUARTER(OrderDate) = 1 THEN '1'
                WHEN QUARTER(OrderDate) = 2 THEN '2'
                WHEN QUARTER(OrderDate) = 3 THEN '3'
                WHEN QUARTER(OrderDate) = 4 THEN '4'
            END) AS QuarterYear
            
        --MONTH
        , MONTH(OrderDate) AS Month
        
        --MONTHNAME
        , CONCAT(LEFT(MONTHNAME(OrderDate), 3), ' ', YEAR(OrderDate)) AS MonthName
        
        --MONTHYEAR
        , CONCAT(YEAR(OrderDate), '-', MONTH(OrderDate)) AS MonthYear
        
        --WEEK
        , CEIL(DAY(orderdate) / 7) AS week
        
        --WEEKNAME
        , CONCAT('Wk-' || CEIL(DAY(ORDERDATE) / 7)) AS WEEK
    
        --YEAR
        , YEAR(ORDERDATE) AS YEAR
        
        --DeliveryTAT
        , DATEDIFF(day,ORDERDATE,SHIPDATE) AS DeliveryTAT
        
        --ShipmentStatus
        , CASE
                WHEN ShipDate < DueDate THEN 'Early'
                WHEN ShipDate = DueDate THEN 'Normal'
                WHEN ShipDate > DueDate THEN 'Late'
        END AS ShipmentStatus
            
        --ProductType
        , CASE
            WHEN MakeFlag=1 THEN 'Manufactured'
            WHEN MakeFlag=0 THEN 'Purchased'
        END AS ProductType

        --OrderValue
        , OrderQty * (UnitPrice - (UnitPrice * UnitPriceDiscount)) AS OrderValue

        -- TaxAmount
        , o.taxamt / COUNT(o.productid) OVER (PARTITION BY o.SalesOrderID) AS TaxAmount
        
        --FreightAmount    
        , o.freight / COUNT(o.productid) OVER (PARTITION BY o.SalesOrderID) AS FreightAmount

        --ExtendedAmount
        , (OrderQty * (UnitPrice - (UnitPrice * UnitPriceDiscount))) + (o.taxamt / COUNT(o.productid) OVER (PARTITION BY o.SalesOrderID)) + (o.freight / COUNT(o.productid) OVER (PARTITION BY o.SalesOrderID)) AS ExtendedAmount
            
        FROM
            Orders o
            JOIN Customers c ON o.CustomerID = c.CustomerID
            JOIN Products p ON o.ProductID = p.ProductID
            JOIN ProductSubCategories psc ON p.SubCategoryID = psc.SubCategoryID
            JOIN ProductCategories pc ON psc.CategoryID = pc.CategoryID
            JOIN VendorProduct vp ON p.ProductID = vp.ProductID
            JOIN Vendors v ON vp.VendorID = v.VendorID
)

SELECT
    * -- Select all columns
    -- SubTotal
    , SUM(ExtendedAmount) OVER (PARTITION BY OrderId ORDER BY OrderId) AS SubTotal
FROM
    Transformations;


-- Retrieve data from the SalesFact table
select * from SalesFact;