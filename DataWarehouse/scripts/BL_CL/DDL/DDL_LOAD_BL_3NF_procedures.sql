/*
   Procedures for Load data from SA layer to BL_3NF layer.
   Procedure for insert default rows in BL_3NF tables.
   
   Run once.
 */

-----------------------INSERT Procedures

----Regions
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_regions()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_regions';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_3NF.CE_REGIONS (
	REGION_ID,
	REGION_SRC_ID,
	REGION_SOURCE_TABLE,
	REGION_SOURCE_SYSTEM,
	REGION_NAME,
	INSERT_DT,
	UPDATE_DT
	)
	SELECT  
		nextval('bl_3nf.ce_regions_region_id_seq'::regclass),
		src.region_src_id,
		src.source_table, 
		src.source_system, 
		src.region, 
		current_date,
		current_date
	FROM (
		SELECT 
			DISTINCT COALESCE(ds.region, 'n.a.') AS region_src_id,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds.region, 'n.a.') AS region
		FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		UNION ALL
		SELECT 
			DISTINCT COALESCE(ms.region, 'n.a.') AS region_src_id,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms.region, 'n.a.') AS region
		FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
		) src
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_REGIONS cr
		WHERE cr.REGION_SRC_ID = src.region_src_id AND 
		  	cr.REGION_SOURCE_TABLE = src.source_table AND 
		  	cr.REGION_SOURCE_SYSTEM = src.source_system 
	);	

	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);
	
	RAISE NOTICE 'ETL: ce_regions data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Countries, with FK to regions
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_countries()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_countries';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_3NF.CE_COUNTRIES (
	COUNTRY_ID,
	COUNTRY_SRC_ID,
	COUNTRY_SOURCE_TABLE,
	COUNTRY_SOURCE_SYSTEM,
	COUNTRY_NAME,
	COUNTRY_CODE,
	COUNTRY_REGION_ID,
	INSERT_DT,
	UPDATE_DT
	)
	SELECT  
		nextval('bl_3nf.ce_countries_country_id_seq'::regclass),
		src.country_src_id,
		src.source_table, 
		src.source_system, 
		src.country,
		src.country_code,
		COALESCE(cr.region_id, -1),
		current_date,
		current_date
	FROM (
		SELECT 
			DISTINCT COALESCE(ds.country_code, 'n.a.') AS country_src_id,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds.country, 'n.a.') AS country,
			COALESCE(ds.country_code, 'n.a.') AS country_code,
			ds.region
		FROM 
			sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		UNION ALL
		SELECT 
			DISTINCT COALESCE(ms.country_code, 'n.a.') AS country_src_id,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms.country, 'n.a.') AS country,
			COALESCE(ms.country_code, 'n.a.') AS country_code,
			ms.region
		FROM 
			sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
	) src
	LEFT JOIN 
		BL_3NF.CE_REGIONS cr ON src.region = cr.region_src_id AND
						 src.source_table = cr.region_source_table AND
						 src.source_system = cr.region_source_system
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_COUNTRIES c
		WHERE c.COUNTRY_SRC_ID = src.country_src_id AND 
		c.COUNTRY_SOURCE_TABLE = src.source_table AND 
		c.COUNTRY_SOURCE_SYSTEM = src.source_system
	);
	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);
	
	RAISE NOTICE 'ETL: ce_countries data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Addresses, with FK to countries
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_addresses()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_addresses';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_3NF.CE_ADDRESSES (
		ADDRESS_ID,
		ADDRESS_SRC_ID,
		ADDRESS_SOURCE_TABLE,
		ADDRESS_SOURCE_SYSTEM,
		ADDRESS_TEXT,
		ADDRESS_POSTCODE,
		ADDRESS_COUNTRY_ID,
		INSERT_DT,
		UPDATE_DT
		)
	SELECT  
		nextval('bl_3nf.ce_addresses_address_id_seq'::regclass),
		src.address_src_id,
		src.source_table, 
		src.source_system, 
		src.address,
		src.postcode,
		COALESCE (cc.country_id, -1),
		current_date,
		current_date
	FROM (
		SELECT 
			DISTINCT COALESCE(ds.address_id, 'n.a.') AS address_src_id,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds.address, 'n.a.') AS address,
			COALESCE(ds.postcode, 'n.a.') AS postcode,
			ds.country_code
		FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		UNION ALL
		SELECT 
			DISTINCT COALESCE(ms.address_id, 'n.a.') AS address_src_id,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms.address, 'n.a.') AS address,
			COALESCE(ms.postcode, 'n.a.') AS postcode,
			ms.country_code
		FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
	) src
	LEFT JOIN 
		BL_3NF.CE_COUNTRIES cc ON src.country_code = cc.country_src_id  AND
						 src.source_table = cc.country_source_table  AND
						 src.source_system = cc.country_source_system 
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_ADDRESSES ca
		WHERE ca.ADDRESS_SRC_ID = src.address_src_id AND 
			  ca.ADDRESS_SOURCE_TABLE = src.source_table AND 
			  ca.ADDRESS_SOURCE_SYSTEM = src.source_system
	);

	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);
	
	RAISE NOTICE 'ETL: ce_addresses data loaded successfully';		
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
END;
$$ LANGUAGE plpgsql; 


---- Customers , with FK to addresses
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_customers()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_customers';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_3NF.CE_CUSTOMERS (
		CUSTOMER_ID,
		CUSTOMER_SRC_ID,
		CUSTOMER_SOURCE_TABLE,
		CUSTOMER_SOURCE_SYSTEM,
		CUSTOMER_NAME,
		CUSTOMER_GENDER,
		CUSTOMER_EMAIL,
		CUSTOMER_BIRTHDATE_DT,
		CUSTOMER_ADDRESS_ID,
		INSERT_DT,
		UPDATE_DT
		)
	SELECT  
		nextval('bl_3nf.ce_customers_customer_id_seq'::regclass),
		src.customer_src_id,
		src.source_table, 
		src.source_system, 
		src.customer_name,
		src.gender,
		src.mail,
		src.birthdate,
		COALESCE (lj.address_id, -1),
		current_date,
		current_date
	FROM (
		SELECT 
			COALESCE(ds.user_id, 'n.a.') AS customer_src_id,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds."NAME", 'n.a.') AS customer_name,
			COALESCE(ds.gender, 'n.a.') AS gender,
			COALESCE(ds."EMAIL", 'n.a.') AS mail,
			COALESCE(ds.birthdate, '1900-01-01')::date AS birthdate, 
			ds.address_id
		FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		GROUP BY ds.user_id, ds."NAME", ds.gender, ds."EMAIL", ds.birthdate, ds.address_id
		UNION ALL
		SELECT 
			COALESCE(ms.address_id, 'n.a.') AS customer_src_id,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms."NAME", 'n.a.') AS customer_name,
			COALESCE(ms.gender, 'n.a.') AS gender,
			COALESCE(ms."EMAIL", 'n.a.') AS mail,
			COALESCE(ms.birthdate, '1900-01-01')::date AS birthdate,
			ms.address_id
		FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
		GROUP BY ms.user_id, ms."NAME", ms.gender, ms."EMAIL", ms.birthdate, ms.address_id
	) src
	LEFT JOIN 
		BL_3NF.CE_ADDRESSES lj ON src.address_id = lj.address_src_id  AND
						 src.source_table = lj.address_source_table  AND
						 src.source_system = lj.address_source_system 
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_CUSTOMERS cus
		WHERE cus.CUSTOMER_SRC_ID = src.customer_src_id AND 
			  cus.CUSTOMER_SOURCE_TABLE = src.source_table AND 
			  cus.CUSTOMER_SOURCE_SYSTEM = src.source_system
	);
	
	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);
	
	RAISE NOTICE 'ETL: ce_customers data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Shipments
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_shipments()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_shipments';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block
	
	-- main INSERT statement
		INSERT INTO BL_3NF.CE_SHIPMENTS (
			SHIPMENT_ID,		
			SHIPMENT_SRC_ID,	
			SHIPMENT_SOURCE_TABLE,	
			SHIPMENT_SOURCE_SYSTEM,	
			SHIPMENT_TRACK_NUMBER,	
			SHIPMENT_DT,		
			SHIPMENT_RECEIVED_DT,	
			INSERT_DT,		
			UPDATE_DT
			)
		SELECT  
			nextval('bl_3nf.ce_shipments_shipment_id_seq'::regclass),
			src.shipment_src_id,
			src.source_table, 
			src.source_system, 
			src.track_number,
			src.shipment_date,
			src.shipment_received,
			current_date,
			current_date
		FROM (
			SELECT 
				COALESCE(ms.track_number, 'n.a.') AS shipment_src_id,
				'SRC_BANDCAMP_MERCH_SALES' AS source_table,
				'SA_BANDCAMP_MERCH_SALES' AS source_system,
				COALESCE(ms.track_number, 'n.a.') AS track_number,
				COALESCE(to_timestamp(ms.shipment_date::bigint)::date, '1900-01-01'::date) AS shipment_date,
				COALESCE(to_timestamp(ms.shipment_received::bigint)::date, '9999-12-31'::date) AS shipment_received
			FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
			GROUP BY ms.track_number, ms.shipment_date, ms.shipment_received
		) src
		WHERE NOT EXISTS (
						SELECT 1
						FROM BL_3NF.CE_SHIPMENTS cs
						WHERE cs.SHIPMENT_SRC_ID = src.shipment_src_id AND 
							  cs.SHIPMENT_SOURCE_TABLE = src.source_table AND 
							  cs.SHIPMENT_SOURCE_SYSTEM = src.source_system
				  		);
	
	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);

	RAISE NOTICE 'ETL: ce_shipments data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Currencies
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_currencies()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_currencies';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_3NF.CE_CURRENCIES (
		CURRENCY_ID,		
		CURRENCY_SRC_ID,	
		CURRENCY_SOURCE_TABLE,	
		CURRENCY_SOURCE_SYSTEM,	
		CURRENCY_CODE,			
		INSERT_DT,		
		UPDATE_DT
		)
	SELECT  
		nextval('bl_3nf.ce_currencies_currency_id_seq'::regclass),
		src.currency_src_id,
		src.source_table, 
		src.source_system, 
		src.currency_code,
		current_date,
		current_date
	FROM (
		SELECT 
			DISTINCT COALESCE(ds.currency, 'n.a.') AS currency_src_id,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds.currency, 'n.a.') AS currency_code
		FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		UNION ALL
		SELECT 
			DISTINCT COALESCE(ms.currency, 'n.a.') AS currency_src_id,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms.currency, 'n.a.') AS currency_code
		FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
	) src
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_CURRENCIES cc
		WHERE cc.CURRENCY_SRC_ID = src.currency_src_id AND 
			  cc.CURRENCY_SOURCE_TABLE = src.source_table AND 
			  cc.CURRENCY_SOURCE_SYSTEM = src.source_system
	);
	
	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);

	RAISE NOTICE 'ETL: ce_currencies data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Promotions
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_promotions()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_promotions';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_3NF.CE_PROMOTIONS (
		PROMOTION_ID,		
		PROMOTION_SRC_ID,	
		PROMOTION_SOURCE_TABLE,	
		PROMOTION_SOURCE_SYSTEM,	
		PROMOTION_NAME,		
		PROMOTION_DESCRIPTION,	
		PROMOTION_FEE_DISCOUNT_PERCENT,	
		INSERT_DT,		
		UPDATE_DT
		)
	SELECT  
		nextval('bl_3nf.ce_promotions_promotion_id_seq'::regclass),
		src.promotion_src_id,
		src.source_table, 
		src.source_system, 
		src.promotion_name,
		src.promotion_description,
		src.promotion_fee_dp,
		current_date,
		current_date
	FROM (
		SELECT 
			DISTINCT COALESCE(ds.promotion_id, 'n.a.') AS promotion_src_id,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds.promotion, 'n.a.') AS promotion_name,
			COALESCE(ds.promotion_description, 'n.a.') AS promotion_description,
			COALESCE(ds.fee_discount::smallint, -1) AS promotion_fee_dp
		FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		UNION ALL
		SELECT 
			DISTINCT COALESCE(ms.promotion_id, 'n.a.') AS promotion_src_id,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms.promotion_name, 'n.a.') AS promotion_name,
			COALESCE(ms.promotion_description, 'n.a.') AS promotion_description,
			COALESCE(ms.fee_discount::smallint, -1) AS promotion_fee_dp
		FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
	) src
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_PROMOTIONS cc
		WHERE cc.PROMOTION_SRC_ID = src.promotion_src_id AND 
			  cc.PROMOTION_SOURCE_TABLE = src.source_table AND 
			  cc.PROMOTION_SOURCE_SYSTEM = src.source_system
	);
	
	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);

	RAISE NOTICE 'ETL: ce_promotions data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Product-types
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_product_types()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_product_types';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_3NF.CE_PRODUCT_TYPES (
		PRODUCT_TYPE_ID,		
		PRODUCT_TYPE_SRC_ID,	
		PRODUCT_TYPE_SOURCE_TABLE,	
		PRODUCT_TYPE_SOURCE_SYSTEM,	
		PRODUCT_TYPE_CODE,		
		INSERT_DT,		
		UPDATE_DT
		)
	SELECT  
		nextval('bl_3nf.ce_product_types_product_type_id_seq'::regclass),
		src.product_type_src_id,
		src.source_table, 
		src.source_system, 
		src.product_type,
		current_date,
		current_date
	FROM (
		SELECT 
			DISTINCT COALESCE(ds.item_type, 'n.a.') AS product_type_src_id,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds.item_type, 'n.a.') AS product_type
		FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		UNION ALL
		SELECT 
			DISTINCT COALESCE(ms.item_type, 'n.a.') AS product_type_src_id,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms.item_type, 'n.a.') AS product_type
		FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
	) src
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_PRODUCT_TYPES pt
		WHERE pt.PRODUCT_TYPE_SRC_ID = src.product_type_src_id AND 
			  pt.PRODUCT_TYPE_SOURCE_TABLE = src.source_table AND 
			  pt.PRODUCT_TYPE_SOURCE_SYSTEM = src.source_system
	);
	
	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);
	
	RAISE NOTICE 'ETL: ce_product_types data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Products with FK to products_types
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_products()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_products';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_3NF.CE_PRODUCTS (
		PRODUCT_ID,		
		PRODUCT_SRC_ID,		
		PRODUCT_SOURCE_TABLE,	
		PRODUCT_SOURCE_SYSTEM,	
		PRODUCT_URL,		
		PRODUCT_DESCRIPTION,	
		PRODUCT_IMAGE_URL,	
		PRODUCT_ALBUM_TITLE,	
		PRODUCT_TYPE_ID,		
		INSERT_DT,		
		UPDATE_DT
		)
	SELECT  
		nextval('bl_3nf.ce_products_product_id_seq'::regclass),
		src.product_src_id,
		src.source_table, 
		src.source_system, 
		src.product_url,
		src.description,
		src.image_url,
		src.album_title,
		COALESCE (lj.product_type_id, -1),
		current_date,
		current_date
	FROM (
		SELECT 
			COALESCE(ds.prod_id, 'n.a.') AS product_src_id,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds.url, 'n.a.') AS product_url,
			COALESCE(ds.item_description, 'n.a.') AS description,
			COALESCE(ds.image_url, 'n.a.') AS image_url,
			COALESCE(ds.album_title, 'n.a.') AS album_title, 
			ds.item_type 
		FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		GROUP BY ds.prod_id, ds.url, ds.item_description, ds.image_url, ds.album_title, ds.item_type 
		UNION ALL
		SELECT 
			COALESCE(ms.prod_id, 'n.a.') AS product_src_id,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms.url, 'n.a.') AS product_url,
			COALESCE(ms.item_description, 'n.a.') AS description,
			COALESCE(ms.image_url, 'n.a.') AS image_url,
			COALESCE(ms.album_title, 'n.a.') AS album_title,
			ms.item_type 
		FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
		GROUP BY ms.prod_id, ms.url, ms.item_description, ms.image_url, ms.album_title, ms.item_type 
	) src
	LEFT JOIN 
		BL_3NF.CE_PRODUCT_TYPES lj ON  src.item_type = lj.PRODUCT_TYPE_SRC_ID  AND
						 		src.source_table = lj.PRODUCT_TYPE_SOURCE_TABLE  AND
						 		src.source_system = lj.PRODUCT_TYPE_SOURCE_SYSTEM 
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_PRODUCTS wn
		WHERE wn.PRODUCT_SRC_ID = src.product_src_id AND 
			  wn.PRODUCT_SOURCE_TABLE = src.source_table AND 
			  wn.PRODUCT_SOURCE_SYSTEM = src.source_system
	);
	
	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);

	RAISE NOTICE 'ETL: ce_products data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Genres
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_genres()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_genres';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_3NF.CE_GENRES (
		GENRE_ID,		
		GENRE_SRC_ID,		
		GENRE_SOURCE_TABLE,	
		GENRE_SOURCE_SYSTEM,	
		GENRE,			
		INSERT_DT,		
		UPDATE_DT
		)
	SELECT  
		nextval('bl_3nf.ce_genres_genre_id_seq'::regclass),
		src.genre_src_id,
		src.source_table, 
		src.source_system, 
		src.genre,
		current_date,
		current_date
	FROM (
		SELECT 
			DISTINCT COALESCE(ds.genre, 'n.a.') AS genre_src_id,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds.genre, 'n.a.') AS genre
		FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		UNION ALL
		SELECT 
			DISTINCT COALESCE(ms.genre, 'n.a.') AS genre_src_id,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms.genre, 'n.a.') AS genre
		FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
	) src
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_GENRES g
		WHERE g.GENRE_SRC_ID = src.genre_src_id AND 
			  g.GENRE_SOURCE_TABLE = src.source_table AND 
			  g.GENRE_SOURCE_SYSTEM = src.source_system
	);
	
	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);

	RAISE NOTICE 'ETL: ce_genres data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Artists. Changing Dimension SCD2.
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_3nf_ce_artists_scd()
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
	v_name := 'bl_cl.proc_insert_bl_3nf_ce_artists_scd';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main UPDATE/INSERT statements
	---- 1. full reload working table for check artist-genre relation in sources
	CALL bl_cl.proc_insert_bl_cl_wrk_atrists_genres();
	
	---- 2. update rows if artist changed his genre
			--find artists, which genre is changed in sources
	WITH changed_genre AS
		(
			SELECT  g.genre_id, g.genre_src_id, 
					g.genre,
					a.*
			FROM  bl_cl.wrk_artists_genres wag
			LEFT JOIN bl_3nf.ce_genres g ON wag.genre_source_id = g.genre_src_id 
										AND wag.source_table  = g.genre_source_table
										AND wag.source_system = g.genre_source_system 
			LEFT JOIN bl_3nf.ce_artists_scd a ON wag.artist_src_id = a.artist_src_id 
											AND wag.source_table  = a.artist_source_table 
											AND wag.source_system = a.artist_source_system
			WHERE g.genre_id <> a.artist_genre_id AND a.is_active = 'Y'      --filter: genre is new for active row only
		),
			--insert row with new information
		insert_changed AS
		(
			INSERT INTO bl_3nf.ce_artists_scd (
								artist_id, artist_src_id, artist_source_table, artist_source_system,	
								artist_name, artist_genre_id, start_dt,	end_dt,	is_active, insert_dt)
			SELECT artist_id, artist_src_id, artist_source_table, artist_source_system,	
								artist_name, genre_id, current_date, '9999-12-31'::date,
								'Y', current_date
			FROM changed_genre
			RETURNING artist_id, artist_genre_id
		)
			--change old row
			UPDATE bl_3nf.ce_artists_scd a
			SET is_active = 'N',
			end_dt = current_date
			WHERE a.artist_id IN (SELECT artist_id FROM insert_changed)
					AND a.is_active = 'Y'
					AND a.artist_genre_id <> (SELECT i.artist_genre_id FROM insert_changed i WHERE a.artist_id = i.artist_id);
				
	---- 3.Insert new rows
	INSERT INTO BL_3NF.CE_ARTISTS_SCD (
		ARTIST_ID,		
		ARTIST_SRC_ID,		
		ARTIST_SOURCE_TABLE,	
		ARTIST_SOURCE_SYSTEM,	
		ARTIST_NAME,		
		ARTIST_GENRE_ID,	
		START_DT,		
		END_DT,			
		IS_ACTIVE,		
		INSERT_DT
		)
	SELECT  
		nextval('bl_3nf.ce_artists_artist_id_seq'::regclass),
		src.artist_src_id,
		src.source_table, 
		src.source_system, 
		src.artist_name,
		src.genre_id,
		'1900-01-01'::date,
		'9999-12-31'::date,
		'Y',
		current_date 
	FROM (
		SELECT 
			ag.artist_src_id,
			ag.source_table,
			ag.source_system,
			ag.artist_name,
			COALESCE(g.genre_id, -1) AS genre_id
		FROM 
			bl_cl.wrk_artists_genres ag
		LEFT JOIN 
		BL_3NF.CE_GENRES g ON  ag.genre = g.genre_src_id  AND
						 ag.source_table = g.genre_source_table AND
						 ag.source_system = g.genre_source_system
	) src
	WHERE NOT EXISTS (
		SELECT 1
		FROM BL_3NF.CE_ARTISTS_SCD wn
		WHERE wn.ARTIST_SRC_ID = src.artist_src_id AND 
		wn.ARTIST_SOURCE_TABLE = src.source_table AND 
		wn.ARTIST_SOURCE_SYSTEM = src.source_system AND
		wn.is_active = 'Y'
	);

	-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);

			
	RAISE NOTICE 'ETL: ce_artists_scd data loaded successfully';			
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
END;
$$ LANGUAGE plpgsql; 


---- Sales. Fact table
CREATE OR REPLACE PROCEDURE bl_cl.proc_incremetal_insert_bl_3nf_ce_sales()
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
	v_name := 'bl_cl.proc_incremetal_insert_bl_3nf_ce_sales';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block
	
	-- Incremental sources by transaction timestamp	
	EXECUTE 'CREATE OR REPLACE VIEW bl_cl.BL_3NF_CE_SALES_INCREMENTAL_LOAD_FROM_DIGITAL_SRC
				AS (SELECT * FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds 
							WHERE to_timestamp(ds.utc_date::double precision) > 
										(SELECT previous_loaded_dt FROM bl_cl.MTA_BL_3NF_CE_SALES_INCREMENTAL_LOAD
					 						WHERE upper(source_system) = ''SA_BANDCAMP_DIGITAL_SALES'' AND 
													upper(source_table) = ''SRC_BANDCAMP_DIGITAL_SALES'')
					)';
	
	EXECUTE 'CREATE OR REPLACE VIEW bl_cl.BL_3NF_CE_SALES_INCREMENTAL_LOAD_FROM_MERCH_SRC
				AS (SELECT * FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms 
							WHERE to_timestamp(ms.utc_date::double precision) > 
										(SELECT previous_loaded_dt FROM bl_cl.MTA_BL_3NF_CE_SALES_INCREMENTAL_LOAD
											 WHERE upper(source_system) = ''SA_BANDCAMP_MERCH_SALES'' AND 
													upper(source_table) = ''SRC_BANDCAMP_MERCH_SALES'')
					)';	
		
	-- main INSERT statement
		INSERT INTO BL_3NF.CE_SALES (
			SALE_ID,
			SALE_SRC_ID,
			SALE_SOURCE_TABLE,
			SALE_SOURCE_SYSTEM,
			SHIPMENT_ID,
			CUSTOMER_ID,
			PRODUCT_ID,
			PROMOTION_ID,
			CURRENCY_ID,
			ARTIST_ID,
			EVENT_DT,
			SALE_UTC,
			SALE_AMOUNT_PAID,
			SALE_AMOUNT_PAID_USD,
			SALE_PRODUCT_PRICE,
			SALE_PRODUCT_TYPE_PLATFORM_FEE_PERCENT,
			SALE_PROMOTION_FEE_DISCOUNT_PERCENT,
			INSERT_DT,
			UPDATE_DT
			)
		SELECT  
			nextval('bl_3nf.ce_sales_sale_id_seq'::regclass),
			src.sale_src_id,
			src.source_table, 
			src.source_system, 
			COALESCE(ship.shipment_id, -1),
			COALESCE(cust.customer_id, -1),
			COALESCE(prod.product_id, -1),
			COALESCE(promo.promotion_id, -1),
			COALESCE(curr.currency_id, -1),
			COALESCE(a.artist_id, -1),
			src.event_date,
			src.utc,
			src.amount_paid,
			src.amount_paid_usd,
			src.product_price,
			src.platform_fee,
			src.fee_discount,
			current_date,
			current_date 
		FROM (
			SELECT 
				ds._id AS sale_src_id,
				'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
				'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
				'n.a.' AS shipment_src_id,                     --there is no shipment for digital sales, always will be 'n.a.'
				COALESCE(ds.user_id, 'n.a.') AS customer_src_id,
				COALESCE(ds.prod_id, 'n.a.') AS product_src_id,
				COALESCE(ds.promotion_id, 'n.a.') AS promotion_src_id,
				COALESCE(ds.currency, 'n.a.') AS currency_src_id,
				COALESCE(ds.artist_id, 'n.a.') AS artist_src_id,
				to_timestamp(ds.utc_date::double precision)::date AS event_date,
				to_timestamp(ds.utc_date::double precision) AS utc,
				ds.amount_paid::NUMERIC(10,2),  
				ds.amount_paid_usd::NUMERIC(10,2),
				ds.item_price::NUMERIC(10,2) AS product_price,
				ds.platform_fee_percent::NUMERIC  AS platform_fee,
				ds.fee_discount::NUMERIC 
			FROM 
				bl_cl.bl_3nf_ce_sales_incremental_load_from_digital_src ds
			UNION ALL
			SELECT 
				ms._id AS sale_src_id,
				'SRC_BANDCAMP_MERCH_SALES' AS source_table,
				'SA_BANDCAMP_MERCH_SALES' AS source_system,
				COALESCE(ms.track_number, 'n.a.') AS shipment_src_id,
				COALESCE(ms.user_id, 'n.a.') AS customer_src_id,
				COALESCE(ms.prod_id, 'n.a.') AS product_src_id,
				COALESCE(ms.promotion_id, 'n.a.') AS promotion_src_id,
				COALESCE(ms.currency, 'n.a.') AS currency_src_id,
				COALESCE(ms.artist_id, 'n.a.') AS artist_src_id,
				to_timestamp(ms.utc_date::numeric)::date AS event_date,
				to_timestamp(ms.utc_date::numeric) AS utc,
				ms.amount_paid::NUMERIC(10,2),
				ms.amount_paid_usd::NUMERIC(10,2),
				ms.item_price::NUMERIC(10,2) AS product_price,
				ms.platform_fee_percent::NUMERIC::SMALLINT  AS platform_fee,
				ms.fee_discount::NUMERIC::smallint
			FROM 
				bl_cl.bl_3nf_ce_sales_incremental_load_from_merch_src ms
		) src
		LEFT JOIN 
			bl_3nf.ce_shipments ship ON src.shipment_src_id = ship.shipment_src_id AND
							 	 src.source_table  = ship.shipment_source_table AND
							 	 src.source_system = ship.shipment_source_system  
		LEFT JOIN 
			bl_3nf.ce_customers cust ON src.customer_src_id = cust.customer_src_id AND
							 	 src.source_table   = cust.customer_source_table AND
							 	 src.source_system  = cust.customer_source_system  
		LEFT JOIN 
			bl_3nf.ce_products prod ON src.product_src_id = prod.product_src_id  AND
							 	 src.source_table = prod.product_source_table AND
							 	 src.source_system = prod.product_source_system				 	 
		LEFT JOIN 
			bl_3nf.ce_promotions promo ON src.promotion_src_id = promo.promotion_src_id AND
							 	 	src.source_table = promo.promotion_source_table AND
							 	 	src.source_system = promo.promotion_source_system 
		LEFT JOIN 
			bl_3nf.ce_currencies curr ON src.currency_src_id = curr.currency_src_id  AND
							 	 src.source_table = curr.currency_source_table AND
							 	 src.source_system = curr.currency_source_system 					 	 
		LEFT JOIN 
			bl_3nf.ce_artists_scd a ON  src.artist_src_id = a.artist_src_id AND
							 	 src.source_table = a.artist_source_table AND
							 	 src.source_system = a.artist_source_system	 
		WHERE a.is_active = 'Y';					 	 
			-- save count of rows affected	  	
	GET DIAGNOSTICS v_rcount := ROW_COUNT;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);
	
	-- UPDATE incremental load info
	UPDATE bl_cl.mta_bl_3nf_ce_sales_incremental_load
	SET previous_loaded_dt = current_timestamp; 

	RAISE NOTICE 'ETL: ce_sales data loaded successfully';	
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

