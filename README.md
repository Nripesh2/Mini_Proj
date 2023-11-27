# Sales_Mini_Project
-- Create the SalesFact table
CREATE OR REPLACE TABLE SalesFact (
    SeqNo INT IDENTITY(1,1),
    OrderId INT,
    OrderDate DATE,
    OrderQty INT,
    UnitPrice DECIMAL(18,2),
    UnitPriceDiscount DECIMAL(18,2),
    ShipDate DATE,
    DueDate DATE,
    CustomerId INT,
    CustomerName VARCHAR(),
    ProductId INT,
    ProductName VARCHAR(),
    ModelName VARCHAR(),
    MakeFlag BOOLEAN,
    StandardCost DECIMAL(18,2),
    ListPrice DECIMAL(18,2),
    CategoryName VARCHAR(),
    SubCategoryName VARCHAR(),
    VendorID INT,
    VendorName VARCHAR(),
    VendorAccountNumber VARCHAR(),
    VendorCreditRating INT,
    VendorActiveFlag BOOLEAN,
    DayType VARCHAR(),
    Day VARCHAR(),
    DayShort VARCHAR(),
    Quarter VARCHAR(),
    QuarterYear VARCHAR(),
    Month INT,
    MonthName VARCHAR(),
    MonthYear VARCHAR(),
    Week INT,
    WeekName VARCHAR(),
    Year INT,
    DeliveryTAT INT,
    ShipmentStatus VARCHAR(),
    ProductType VARCHAR(),
    OrderValue DECIMAL(18,2)
    --TaxAmount DECIMAL(18,2),
    --FreightAmount DECIMAL(18,2),
    --ExtendedAmount DECIMAL(18,2),
    --SubTotal DECIMAL(18,2)
);

-- Populate the SalesFact table with data
INSERT INTO SalesFact (
    OrderId, OrderDate, OrderQty, UnitPrice, UnitPriceDiscount,
    ShipDate, DueDate, CustomerId, CustomerName,
    ProductId, ProductName, ModelName, MakeFlag, StandardCost, ListPrice,
    CategoryName, SubCategoryName, VendorID, VendorName,
    VendorAccountNumber, VendorCreditRating, VendorActiveFlag,
    DayType,Day,DayShort,Quarter,QuarterYear,Month,MonthYear,MonthName,WEEK,WeekName,Year,
    DeliveryTAT,ShipmentStatus,ProductType,OrderValue--,TaxAmount,FreightAmount
    -- ExtendedAmount,SubTotal
)
SELECT
    o.SalesOrderID AS OrderId,
    o.OrderDate,
    o.OrderQty,
    o.UnitPrice,
    o.UnitPriceDiscount,
    o.ShipDate,
    o.DueDate,
    c.CustomerId AS CustomerId,
    c.FullName AS CustomerName,
    p.ProductId,
    p.ProductName,
    p.ModelName,
    p.MakeFlag,
    p.StandardCost,
    p.ListPrice,
    pc.CategoryName,
    psc.SubCategoryName,
    vp.VendorID,
    v.VendorName,
    v.AccountNumber AS VendorAccountNumber,
    v.CreditRating AS VendorCreditRating,
    v.ActiveFlag AS VendorActiveFlag,
    --DAYTYPE
    CASE WHEN DAYOFWEEK(o.OrderDate) IN (2, 3, 4, 5, 6) THEN 'Weekday' ELSE 'Weekend' END AS DayType,
    --DAY
    CASE 
        WHEN DAYOFWEEK(OrderDate) = 1 THEN 'Sunday'
        WHEN DAYOFWEEK(OrderDate) = 2 THEN 'Monday'
        WHEN DAYOFWEEK(OrderDate) = 3 THEN 'Tuesday'
        WHEN DAYOFWEEK(OrderDate) = 4 THEN 'Wednesday'
        WHEN DAYOFWEEK(OrderDate) = 5 THEN 'Thursday'
        WHEN DAYOFWEEK(OrderDate) = 6 THEN 'Friday'
        WHEN DAYOFWEEK(OrderDate) = 7 THEN 'Saturday'
    END AS Day,
    
    --DAYSHORT
        CASE 
        WHEN DAYOFWEEK(OrderDate) = 1 THEN 'Sun'
        WHEN DAYOFWEEK(OrderDate) = 2 THEN 'Mon'
        WHEN DAYOFWEEK(OrderDate) = 3 THEN 'Tue'
        WHEN DAYOFWEEK(OrderDate) = 4 THEN 'Wed'
        WHEN DAYOFWEEK(OrderDate) = 5 THEN 'Thu'
        WHEN DAYOFWEEK(OrderDate) = 6 THEN 'Fri'
        WHEN DAYOFWEEK(OrderDate) = 7 THEN 'Sat'
    END AS DayShort,
    CASE 
        WHEN QUARTER(OrderDate) = 1 THEN 'Q1'
        WHEN QUARTER(OrderDate) = 2 THEN 'Q2'
        WHEN QUARTER(OrderDate) = 3 THEN 'Q3'
        WHEN QUARTER(OrderDate) = 4 THEN 'Q4'
    END AS Quarter,
    
    --QUATERYEAR
        CONCAT(YEAR(OrderDate), '-Q',
        CASE 
            WHEN QUARTER(OrderDate) = 1 THEN '1'
            WHEN QUARTER(OrderDate) = 2 THEN '2'
            WHEN QUARTER(OrderDate) = 3 THEN '3'
            WHEN QUARTER(OrderDate) = 4 THEN '4'
        END) AS QuarterYear,
        
    --MONTH
    MONTH(OrderDate) AS Month,
    
    --MONTHNAME
    CONCAT(LEFT(MONTHNAME(OrderDate), 3), ' ', YEAR(OrderDate)) AS MonthName,
    
    --MONTHYEAR
    CONCAT(YEAR(OrderDate), '-', MONTH(OrderDate)) AS MonthYear,
    
    --WEEK
    CEIL(DAY(orderdate) / 7) AS week,
    
    --WEEKNAME
    CONCAT('Wk-' || CEIL(DAY(ORDERDATE) / 7)) AS WEEK,

    --YEAR
    YEAR(ORDERDATE) AS YEAR,
    
    --DeliveryTAT
    DATEDIFF(day,ORDERDATE,SHIPDATE) AS DeliveryTAT,
    
    --ShipmentStatus
        CASE
            WHEN ShipDate < DueDate THEN 'Early'
            WHEN ShipDate = DueDate THEN 'Normal'
            WHEN ShipDate > DueDate THEN 'Late'
        END AS ShipmentStatus,
        
    --ProductType
        CASE
        WHEN MakeFlag=1 THEN 'Manufactured'
        WHEN MakeFlag=0 THEN 'Purchased'
    END AS ProductType,
    
    --OrderValue
    OrderQty * (UnitPrice - (UnitPrice * UnitPriceDiscount)) AS OrderValue
    
    --TaxAmount
    --SUM(o.TaxAmt) AS TaxAmount,
    
    --FreightAmount
    --SUM(o.Freight) AS FreightAmount
    

FROM
    Orders o
    JOIN Customers c ON o.CustomerID = c.CustomerID
    JOIN Products p ON o.ProductID = p.ProductID
    JOIN ProductSubCategories psc ON p.SubCategoryID = psc.SubCategoryID
    JOIN ProductCategories pc ON psc.CategoryID = pc.CategoryID
    JOIN VendorProduct vp ON p.ProductID = vp.ProductID
    JOIN Vendors v ON vp.VendorID = v.VendorID;
    
-- Add additional calculations for DayType... 
SELECT * FROM SalesFact;

