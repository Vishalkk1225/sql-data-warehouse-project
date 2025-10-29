/*
========================================================
Create database and Schemas
========================================================
Script Purpose: 
  This Script creates a new database named 'dataWarehouse' after checking it alredy exists.  
  If the Database exists, it is dropped and recreated. Additionally, the scripts set up three schemas 
  within the database: 'bronze', 'silver', and 'gold'.

WARNING:
Running this script will drop the entire 'DataWarehouse' database if it exists.
All data in the database will be permanently deleted. Proceed with caution
and ensure you have proper backups before running this script.

*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;
GO

-- Create Datawarehouse 

create database Datawarehouse;
GO

 use datawarehouse;
 GO

 -- Creating SCHEMAS

 CREATE SCHEMA bronze;
 GO
 CREATE SCHEMA silver;
 GO
 CREATE SCHEMA gold;
GO
 
