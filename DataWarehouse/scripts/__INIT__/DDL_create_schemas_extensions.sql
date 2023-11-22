/* Run once after datbase creation/connection.
  
   Create all neccessary schemas.*/

--Clenasing level BL_CL
CREATE SCHEMA IF NOT EXISTS BL_CL;

--Sources level SA
CREATE EXTENSION IF NOT EXISTS file_fdw;

CREATE SERVER IF NOT EXISTS svr_bandcamp_sales FOREIGN DATA WRAPPER file_fdw;

CREATE SCHEMA IF NOT EXISTS SA_BANDCAMP_DIGITAL_SALES;

CREATE SCHEMA IF NOT EXISTS SA_BANDCAMP_MERCH_SALES;

--Normalized BL_3NF level
CREATE SCHEMA IF NOT EXISTS BL_3NF;

--Star schema BL_DM
CREATE SCHEMA IF NOT EXISTS BL_DM;

