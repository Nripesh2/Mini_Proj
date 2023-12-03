-- Table defining different product categories
CREATE OR REPLACE TABLE ProductCategories (
    CategoryID INT, -- Unique identifier for the category
    CategoryName VARCHAR() -- Name of the category
);

-- Table storing sub-categories related to products
CREATE OR REPLACE TABLE ProductSubCategories (
    SubCategoryID INT, -- Unique identifier for the sub-category
    CategoryID INT, -- Identifier linking to the category it belongs to
    SubCategoryName VARCHAR() -- Name of the sub-category
);

-- Table containing detailed product information
CREATE OR REPLACE TABLE Products (
    ProductID INT, -- Unique identifier for the product
    ProductNumber VARCHAR(), -- Product number/code
    ProductName VARCHAR(), -- Name of the product
    ModelName VARCHAR(), -- Model of the product
    MakeFlag BOOLEAN, -- Indicates whether the product is manufactured
    StandardCost DECIMAL(10, 2), -- Standard cost of the product
    ListPrice DECIMAL(10, 2), -- List price of the product
    SubCategoryID INT -- Identifier linking to the sub-category of the product
);

-- Table storing customer information
CREATE OR REPLACE TABLE Customers (
    CustomerID INT PRIMARY KEY, -- Unique identifier for the customer (Primary Key)
    FirstName VARCHAR(), -- Customer's first name
    LastName VARCHAR(), -- Customer's last name
    FullName VARCHAR() -- Full name of the customer
);

-- Table containing vendor details
CREATE OR REPLACE TABLE Vendors (
    VendorID INT, -- Unique identifier for the vendor
    VendorName VARCHAR(), -- Name of the vendor
    AccountNumber VARCHAR(), -- Vendor's account number
    CreditRating INT, -- Credit rating of the vendor
    ActiveFlag BOOLEAN -- Indicates if the vendor is active
);

-- Table defining the relationship between vendors and products
CREATE OR REPLACE TABLE VendorProduct (
    ProductID INT, -- Identifier for the product
    VendorID INT -- Identifier for the vendor
);

-- Table storing employee details
CREATE OR REPLACE TABLE Employees (
    EmployeeID INT PRIMARY KEY, -- Unique identifier for the employee (Primary Key)
    ManagerID INT, -- Identifier for the manager
    FirstName VARCHAR(), -- Employee's first name
    LastName VARCHAR(), -- Employee's last name
    FullName VARCHAR, -- Employee's full name
    JobTitle VARCHAR(), -- Job title of the employee
    OrganizationLevel INT, -- Level in the organizational hierarchy
    MaritalStatus CHAR(), -- Marital status of the employee
    Gender CHAR(), -- Gender of the employee
    Territory VARCHAR(), -- Territory the employee belongs to
    Country VARCHAR(), -- Country of the employee
    `Group` VARCHAR() -- Group the employee belongs to
);

-- Table storing order details
CREATE OR REPLACE TABLE Orders (
    SalesOrderID INT, -- Unique identifier for the sales order
    SalesOrderDetailID INT, -- Identifier for the sales order detail
    OrderDate DATE, -- Date the order was placed
    DueDate DATE, -- Due date for the order
    ShipDate DATE, -- Date the order was shipped
    EmployeeID INT, -- Identifier for the employee associated with the order
    CustomerID INT, -- Identifier for the customer placing the order
    SubTotal DECIMAL(18, 2), -- Subtotal amount for the order
    TaxAmt DECIMAL(18, 2), -- Tax amount for the order
    Freight DECIMAL(18, 2), -- Freight cost for the order
    TotalDue DECIMAL(18, 2), -- Total amount due for the order
    ProductID INT, -- Identifier for the product in the order
    OrderQty INT, -- Quantity of the product ordered
    UnitPrice DECIMAL(18, 2), -- Unit price of the product
    UnitPriceDiscount DECIMAL(18, 2), -- Discount applied to the unit price
    LineTotal DECIMAL(18, 2) -- Total amount for the line item
);

-- Table storing summarized order details with ETL metadata
CREATE OR REPLACE TABLE OrdersMaster (
    SalesOrderID INT, -- Unique identifier for the sales order
    SalesOrderDetailID INT, -- Identifier for the sales order detail
    OrderDate DATE, -- Date the order was placed
    DueDate DATE, -- Due date for the order
    ShipDate DATE, -- Date the order was shipped
    EmployeeID INT, -- Identifier for the employee associated with the order
    CustomerID INT, -- Identifier for the customer placing the order
    SubTotal DECIMAL(18, 2), -- Subtotal amount for the order
    TaxAmt DECIMAL(18, 2), -- Tax amount for the order
    Freight DECIMAL(18, 2), -- Freight cost for the order
    TotalDue DECIMAL(18, 2), -- Total amount due for the order
    ProductID INT, -- Identifier for the product in the order
    OrderQty INT, -- Quantity of the product ordered
    UnitPrice DECIMAL(18, 2), -- Unit price of the product
    UnitPriceDiscount DECIMAL(18, 2), -- Discount applied to the unit price
    LineTotal DECIMAL(18, 2), -- Total amount for the line item
    SourceSystem VARCHAR(), -- Source system from where data is extracted
    ETL_Date DATE, -- Date of ETL operation
    ETL_User VARCHAR(), -- User performing the ETL operation
    ETL_Delete_Flag BOOLEAN, -- Flag indicating deletion in ETL process
    ETL_Task_Name VARCHAR() -- Name of the ETL task
);
