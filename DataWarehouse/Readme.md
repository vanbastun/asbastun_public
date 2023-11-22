# DataWarehouse Project

Welcome! This project focuses on developing a Data Warehouse for an online music platform that empowers independent artists and labels to directly sell their music to fans. This README file provides an overview of the project structure and organization.

## Project Structure

The project is structured into the following folders:

1. **docs**: This folder contains: 
			1) Business Template that encompasses the business description, DWH architecture, and ETL pipeline.
			2) source files examples (100 rows per file)

2. **scripts**: Inside this folder, you will find SQL scripts responsible for creating the Data Warehouse in the database. The scripts are organized based on the DWH schemas. Each schema folder contains DDL scripts relevant to that specific schema. Additionally, all DML and ETL operations are handled through calling procedures, which are located in the BL_CL (Business Layer - Cleansing Layer) folder.

   - **__INIT__**: This folder contains the initial script required to create all the necessary schemas.

   - **SA**: The SA folder includes DDL scripts related to the source layer.

   - **BL_3NF**: This folder contains DDL scripts for the normalized business layer.

   - **BL_DM**: The BL_DM folder consists of DDL scripts for the dimensional-facts business layer.

   - **BL_CL**: Here, you will find scripts for the Cleansing Layer, including DDL, DCL, and DML (ETL) scripts.


## Getting Started

To get started with the project, please refer to the documentation in the **docs** folder. It provides detailed information about the business requirements, the architecture of the Data Warehouse, and the ETL pipeline. Additionally, the SQL scripts in the **scripts** folder can be used to set up the Data Warehouse in your database.

