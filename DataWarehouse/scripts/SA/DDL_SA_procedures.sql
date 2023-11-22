/* Procedures to create sources tables from sources files.
  Procedures for create external tables should get path to file as an argument, 
  example: 'D:\dwh\bandcamp_digital_sales.csv'. NB: delimeter here for Windows OS. */

-- procedure(path_to_file) to create foreign table from source file 1
CREATE OR REPLACE PROCEDURE bl_cl.proc_create_ext_digital(IN filename_path text)
AS $$
DECLARE 
v_datetime timestamptz;
v_name varchar;
v_rcount int;
v_message varchar;
v_sqlstate varchar(5); 
BEGIN
	-- assign variables inside in order they be visible for Exception block
	v_datetime := now();
	v_name := 'bl_cl.proc_create_ext_digital';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block
	
	EXECUTE 'DROP FOREIGN TABLE IF EXISTS SA_BANDCAMP_DIGITAL_SALES.EXT_DIGITAL_SALES';

    EXECUTE format(
    'CREATE FOREIGN TABLE SA_BANDCAMP_DIGITAL_SALES.EXT_DIGITAL_SALES 
	(
	_ID VARCHAR(2550), 
	IMAGE_URL VARCHAR(255),
	ITEM_TYPE VARCHAR(255),
	UTC_DATE VARCHAR(255),
	COUNTRY_CODE VARCHAR(255),
	COUNTRY VARCHAR(255),
	ITEM_PRICE VARCHAR(255),
	ITEM_DESCRIPTION VARCHAR(2550),
	ART_ID VARCHAR(255),
	URL VARCHAR(255),
	AMOUNT_PAID VARCHAR(255),
	ARTIST_NAME VARCHAR(255),
	CURRENCY VARCHAR(255),
	ALBUM_TITLE VARCHAR(2550),
	AMOUNT_PAID_USD VARCHAR(255),
	REGION VARCHAR(255),
	ARTIST_ID VARCHAR(255),
	GENRE VARCHAR(255),
	USER_ID VARCHAR(255),
	"NAME" VARCHAR(255),
	GENDER VARCHAR(255),
	ADDRESS VARCHAR(1000),
	ADDRESS_ID VARCHAR(255),
	"EMAIL" VARCHAR(255),
	BIRTHDATE VARCHAR(255),
	PROMOTION VARCHAR(255),
	PROMOTION_ID VARCHAR(255),
	PROMOTION_DESCRIPTION VARCHAR(2550),
	PLATFORM_FEE_PERCENT VARCHAR(255),
	FEE_DISCOUNT VARCHAR(255),
	POSTCODE VARCHAR(255),
	PROD_ID VARCHAR(255)
	)
	SERVER svr_bandcamp_sales
	OPTIONS (filename %L,
		format ''csv'',
		HEADER ''true'');', filename_path);

	-- provide logging
	SELECT count(*) FROM SA_BANDCAMP_DIGITAL_SALES.EXT_DIGITAL_SALES INTO v_rcount;
	
	IF v_rcount > 0 THEN
		v_message := 'Data loaded from source file';
	ELSE
		v_message := 'No data found';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);
			
 -- handle exceptions			
EXCEPTION 
	WHEN SQLSTATE '42P01' THEN
		RAISE NOTICE 'Check relation`s names!';
	
		v_message := SQLERRM;
		v_sqlstate := SQLSTATE;
		
		CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime, v_name, v_rcount, v_message, v_sqlstate);
			
	WHEN OTHERS THEN
		v_message := SQLERRM;
		v_sqlstate := SQLSTATE;
		
		CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime, v_name, v_rcount, v_message, v_sqlstate);
		RAISE;
END;
$$ LANGUAGE plpgsql;

---------------------------------------------------------------
--Second EXT_table creation

-- procedure(path_to_file) to create foreign table from source file 2
CREATE OR REPLACE PROCEDURE bl_cl.proc_create_ext_merch(IN filename_path text)
AS $$
DECLARE 
v_datetime timestamptz;
v_name varchar;
v_rcount int;
v_message varchar;
v_sqlstate varchar(5); 
BEGIN
	-- assign variables inside in order they be visible for Exception block
	v_datetime := now();
	v_name := 'bl_cl.proc_create_ext_merch';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	EXECUTE 'DROP FOREIGN TABLE IF EXISTS SA_BANDCAMP_MERCH_SALES.EXT_MERCH_SALES';
	
    EXECUTE format(
    'CREATE FOREIGN TABLE SA_BANDCAMP_MERCH_SALES.EXT_MERCH_SALES 
	(
	_ID VARCHAR(2550), 
	IMAGE_URL VARCHAR(255),
	ITEM_TYPE VARCHAR(255),
	UTC_DATE VARCHAR(255),
	COUNTRY_CODE VARCHAR(255),
	COUNTRY VARCHAR(255),
	ITEM_PRICE VARCHAR(255),
	ITEM_DESCRIPTION VARCHAR(2550),
	ART_ID VARCHAR(255),
	URL VARCHAR(255),
	AMOUNT_PAID VARCHAR(255),
	ARTIST_NAME VARCHAR(255),
	CURRENCY VARCHAR(255),
	ALBUM_TITLE VARCHAR(2550),
	AMOUNT_PAID_USD VARCHAR(255),
	PACKAGE_IMAGE_ID VARCHAR(255),
	REGION VARCHAR(255),
	ARTIST_ID VARCHAR(255),
	GENRE VARCHAR(255),
	USER_ID VARCHAR(255),
	"NAME" VARCHAR(255),
	GENDER VARCHAR(255),
	ADDRESS VARCHAR(1000),
	ADDRESS_ID VARCHAR(255),
	"EMAIL" VARCHAR(255),
	BIRTHDATE VARCHAR(255),
	PROMOTION_ID VARCHAR(255),
	PROMOTION_DESCRIPTION VARCHAR(2550),
	PROMOTION_NAME VARCHAR(255),
	TRACK_NUMBER VARCHAR(255),
	SHIPMENT_DATE VARCHAR(255),
	SHIPMENT_RECEIVED VARCHAR(255),
	PLATFORM_FEE_PERCENT VARCHAR(255),
	FEE_DISCOUNT VARCHAR(255),
	POSTCODE VARCHAR(255),
	PROD_ID VARCHAR(255)
	)
	SERVER svr_bandcamp_sales
	OPTIONS (filename %L,
		format ''csv'',
		HEADER ''true'');', filename_path);
	
	-- provide logging
	SELECT count(*) FROM SA_BANDCAMP_MERCH_SALES.EXT_MERCH_SALES INTO v_rcount;
	
	IF v_rcount > 0 THEN
		v_message := 'Data loaded from source file';
	ELSE
		v_message := 'No data found';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);
			
 -- handle exceptions			
EXCEPTION 
	WHEN SQLSTATE '42P01' THEN
		RAISE NOTICE 'Check relation`s names!';
	
		v_message := SQLERRM;
		v_sqlstate := SQLSTATE;
		
		CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime, v_name, v_rcount, v_message, v_sqlstate);
			
	WHEN OTHERS THEN
		v_message := SQLERRM;
		v_sqlstate := SQLSTATE;
		
		CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime, v_name, v_rcount, v_message, v_sqlstate);
		RAISE;
END;
$$ LANGUAGE plpgsql;

----------------------------------------------------------------
--Procedure for full reload sources tables from files.
CREATE OR REPLACE PROCEDURE bl_cl.proc_full_reload_source_tables()
AS $$
DECLARE 
v_datetime timestamptz;
v_name varchar;
v_rcount int := 0;
v_message varchar;
v_sqlstate varchar(5); 
BEGIN
	-- assign variables inside in order they be visible for Exception block
	v_datetime := now();
	v_name := 'bl_cl.proc_full_reload_source_tables';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

    -- 1.1 Drop table if it exists
    EXECUTE 'DROP TABLE IF EXISTS SA_BANDCAMP_DIGITAL_SALES.SRC_BANDCAMP_DIGITAL_SALES CASCADE';

    -- 1.2 Create 1st source table
    EXECUTE 'CREATE TABLE SA_BANDCAMP_DIGITAL_SALES.SRC_BANDCAMP_DIGITAL_SALES AS TABLE SA_BANDCAMP_DIGITAL_SALES.EXT_DIGITAL_SALES';
   
    RAISE NOTICE 'ETL: SRC_BANDCAMP_DIGITAL_SALES table reloaded successfully.';
   
   	v_rcount := v_rcount + COALESCE((SELECT count(*) FROM SA_BANDCAMP_DIGITAL_SALES.SRC_BANDCAMP_DIGITAL_SALES), 0);
   
    -- 2.1 Drop table if it exists
    EXECUTE 'DROP TABLE IF EXISTS SA_BANDCAMP_MERCH_SALES.SRC_BANDCAMP_MERCH_SALES CASCADE';
  
    -- 2.2 Create 2nd source table
    EXECUTE 'CREATE TABLE SA_BANDCAMP_MERCH_SALES.SRC_BANDCAMP_MERCH_SALES AS TABLE SA_BANDCAMP_MERCH_SALES.EXT_MERCH_SALES';
    
    RAISE NOTICE 'ETL: SRC_BANDCAMP_MERCH_SALES table reloaded successfully.';
   
    v_rcount := v_rcount + COALESCE((SELECT count(*) FROM SA_BANDCAMP_MERCH_SALES.SRC_BANDCAMP_MERCH_SALES), 0);
   	
    IF v_rcount > 0 THEN
		v_message := 'Data loaded in sources tables';
	ELSE
		v_message := 'No data found';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);
			
 -- handle exceptions			
EXCEPTION 
	WHEN SQLSTATE '42P01' THEN
		RAISE NOTICE 'Check relation`s names!';
	
		v_message := SQLERRM;
		v_sqlstate := SQLSTATE;
		
		CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime, v_name, v_rcount, v_message, v_sqlstate);
			
	WHEN OTHERS THEN
		v_message := SQLERRM;
		v_sqlstate := SQLSTATE;
		
		CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime, v_name, v_rcount, v_message, v_sqlstate);
		RAISE;
END;
$$ LANGUAGE plpgsql;
