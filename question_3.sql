-- Step 1: Create Partition Functions

-- For tblTransaction
CREATE PARTITION FUNCTION PF_tblTransactionDate (DATETIME)
AS RANGE RIGHT FOR VALUES (
    '2020-12-31', '2021-12-31', '2022-12-31',
    '2023-12-31', '2024-12-31', '2025-12-31'
);

-- For tblCustomer
CREATE PARTITION FUNCTION PF_tblCustomerRegDate (DATETIME)
AS RANGE RIGHT FOR VALUES (
    '2020-12-31', '2021-12-31', '2022-12-31',
    '2023-12-31', '2024-12-31', '2025-12-31'
);

-- Step 2: Create Partition Schemes

-- For tblTransaction
CREATE PARTITION SCHEME PS_tblTransactionDate
AS PARTITION PF_tblTransactionDate
ALL TO ([PRIMARY]);

-- For tblCustomer
CREATE PARTITION SCHEME PS_tblCustomerRegDate
AS PARTITION PF_tblCustomerRegDate
ALL TO ([PRIMARY]);

-- Step 3: Create Partitioned Tables

-- tblTransaction_Partitioned
CREATE TABLE tblTransaction_Partitioned (
    Transaction_ID BIGINT NOT NULL,
    Amount FLOAT,
    Status VARCHAR(50),
    Customer_ID BIGINT,
    [Created Date] DATETIME
)
ON PS_tblTransactionDate ([Created Date]);

-- tblCustomer_Partitioned
CREATE TABLE tblCustomer_Partitioned (
    Customer_ID BIGINT NOT NULL,
    [First Name] VARCHAR(50),
    [Last Name] VARCHAR(50),
    Country_ID BIGINT,
    [Registration Date] DATETIME
)
ON PS_tblCustomerRegDate ([Registration Date]);

-- tblCountry (Partitioning not required)
CREATE TABLE tblCountry (
    Country_ID BIGINT NOT NULL,
    [Full Name] VARCHAR(100),
    Code VARCHAR(10)
);
