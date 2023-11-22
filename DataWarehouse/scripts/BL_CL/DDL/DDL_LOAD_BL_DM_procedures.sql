/*
   Create procedures for Load data from BL_3NF layer to BL_DM layer.
   Run once.
 */

-----------------------INSERT Procedures----------------------------
--Customers
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_dm_dim_customers()
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
	v_name := 'bl_cl.proc_insert_bl_dm_dim_customers';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	-- main INSERT statement
	INSERT INTO BL_DM.DIM_CUSTOMERS(
			CUSTOMER_SURR_ID, CUSTOMER_SRC_ID, 
			CUSTOMER_SOURCE_TABLE, CUSTOMER_SOURCE_SYSTEM, 
			CUSTOMER_NAME, CUSTOMER_GENDER, CUSTOMER_EMAIL, 
			CUSTOMER_BIRTHDATE_DT, CUSTOMER_ADDRESS_SRC_ID, 
			CUSTOMER_ADDRESS_TEXT, CUSTOMER_ADDRESS_POSTCODE, 
			CUSTOMER_COUNTRY_SRC_ID, CUSTOMER_COUNTRY_NAME, 
			CUSTOMER_COUNTRY_CODE, CUSTOMER_REGION_SRC_ID, 
			CUSTOMER_REGION_NAME, INSERT_DT, UPDATE_DT
			)
	SELECT
		nextval('bl_dm.dim_customers_customer_surr_id_seq'::regclass),
		src.customer_src_id, 
		src.source_table, 
		src.source_system, 
		src.customer_name, 
		src.customer_gender, 
		src.customer_email, 
		src.customer_birthdate_dt, 
		src.customer_address_src_id, 
		src.customer_address_text, 
		src.customer_address_postcode, 
		src.customer_country_src_id, 
		src.customer_country_name, 
		src.customer_country_code, 
		src.customer_region_src_id, 
		src.customer_region_name,
		current_date,
		current_date
	FROM (
		  SELECT
		  		cc.customer_id::varchar AS customer_src_id,
		  		'CE_CUSTOMERS' AS source_table,
				'BL_3NF' AS source_system,
				cc.customer_name,
				cc.customer_gender,
				cc.customer_email, 
				cc.customer_birthdate_dt, 
				ca.address_id::varchar AS customer_address_src_id,
				ca.address_text AS customer_address_text, 
				ca.address_postcode AS customer_address_postcode, 
				cn.country_id::varchar AS customer_country_src_id, 
				cn.country_name AS customer_country_name, 
				cn.country_code AS customer_country_code, 
				cr.region_id::varchar AS customer_region_src_id, 
				cr.region_name AS customer_region_name
		  FROM 		
		  			bl_3nf.ce_customers cc
		  LEFT JOIN bl_3nf.ce_addresses ca ON ca.address_id = cc.customer_address_id 
		  LEFT JOIN bl_3nf.ce_countries cn ON cn.country_id = ca.address_country_id 
		  LEFT JOIN bl_3nf.ce_regions cr   ON cr.region_id = cn.country_region_id 
		  WHERE cc.customer_id <> -1
		  )src
		  WHERE NOT EXISTS (
			SELECT 1
			FROM BL_DM.DIM_CUSTOMERS wn
			WHERE wn.CUSTOMER_SRC_ID    = src.customer_src_id AND 
		  		  wn.CUSTOMER_SOURCE_TABLE = src.source_table AND 
		  		  wn.CUSTOMER_SOURCE_SYSTEM = src.source_system 
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

	RAISE NOTICE 'ETL: dim_customers data loaded successfully';			
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


-- Products
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_dm_dim_products()
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
	v_name := 'bl_cl.proc_insert_bl_dm_dim_products';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block
	
	-- main INSERT statement
	INSERT INTO BL_DM.DIM_PRODUCTS(
			PRODUCT_SURR_ID, PRODUCT_SRC_ID, 
			PRODUCT_SOURCE_TABLE, PRODUCT_SOURCE_SYSTEM, 
			PRODUCT_URL, PRODUCT_DESCRIPTION, PRODUCT_IMAGE_URL, 
			PRODUCT_ALBUM_TITLE, PRODUCT_TYPE_SRC_ID, 
			PRODUCT_TYPE_CODE, INSERT_DT, UPDATE_DT
			)
	SELECT
		nextval('bl_dm.dim_products_product_surr_id_seq'::regclass),
		src.product_src_id, 
		src.source_table, 
		src.source_system, 
		src.product_url, 
		src.product_description, 
		src.product_image_url, 
		src.product_album_title, 
		src.product_type_src_id, 
		src.product_type_code, 
		current_date,
		current_date
	FROM (
		  SELECT
		  		cp.product_id::varchar AS product_src_id,
		  		'CE_PRODUCTS' AS source_table,
				'BL_3NF' AS source_system,
				cp.product_url,
				cp.product_description,
				cp.product_image_url, 
				cp.product_album_title, 
				pt.product_type_id::varchar AS product_type_src_id,
				pt.product_type_code AS product_type_code
		  FROM 		
		  			bl_3nf.ce_products cp
		  LEFT JOIN bl_3nf.ce_product_types pt ON pt.product_type_id = cp.product_type_id  
		  WHERE cp.product_id <> -1
		  )src
		  WHERE NOT EXISTS (
			SELECT 1
			FROM BL_DM.DIM_PRODUCTS wn
			WHERE wn.PRODUCT_SRC_ID    = src.product_src_id AND 
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

	RAISE NOTICE 'ETL: dim_products data loaded successfully';		
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


--Shipments
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_dm_dim_shipments()
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
	v_name := 'bl_cl.proc_insert_bl_dm_dim_shipments';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block
	
	-- main INSERT statement
	INSERT INTO BL_DM.DIM_SHIPMENTS(
			SHIPMENT_SURR_ID, SHIPMENT_SRC_ID, 
			SHIPMENT_SOURCE_TABLE, SHIPMENT_SOURCE_SYSTEM, 
			SHIPMENT_TRACK_NUMBER, SHIPMENT_DT, 
			SHIPMENT_RECEIVED_DT, INSERT_DT, UPDATE_DT
			)
	SELECT
		nextval('bl_dm.dim_shipments_shipment_surr_id_seq'::regclass),
		src.shipment_src_id, 
		src.source_table, 
		src.source_system, 
		src.shipment_track_number, 
		src.shipment_dt, 
		src.shipment_received_dt,
		current_date,
		current_date
	FROM (
		  SELECT
		  		s.shipment_id::varchar AS shipment_src_id,
		  		'CE_SHIPMENTS' AS source_table,
				'BL_3NF' AS source_system,
				s.shipment_track_number,
				s.shipment_dt,
				s.shipment_received_dt 
		  FROM 		
		  		bl_3nf.ce_shipments s
		  WHERE s.shipment_id <> -1
		  )src
		  WHERE NOT EXISTS (
			SELECT 1
			FROM BL_DM.DIM_SHIPMENTS wn
			WHERE wn.SHIPMENT_SRC_ID    = src.shipment_src_id AND 
		  		  wn.SHIPMENT_SOURCE_TABLE = src.source_table AND 
		  		  wn.SHIPMENT_SOURCE_SYSTEM = src.source_system 
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

	RAISE NOTICE 'ETL: dim_shipments data loaded successfully';		
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


-- Currencies
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_dm_dim_currencies()
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
	v_name := 'bl_cl.proc_insert_bl_dm_dim_currencies';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block
	
	-- main INSERT statement
	INSERT INTO BL_DM.DIM_CURRENCIES(
			CURRENCY_SURR_ID, CURRENCY_SRC_ID,
			CURRENCY_SOURCE_TABLE, CURRENCY_SOURCE_SYSTEM,
			CURRENCY_CODE, INSERT_DT, UPDATE_DT
			)
	SELECT
		nextval('bl_dm.dim_currencies_currency_surr_id_seq'::regclass),
		src.currency_src_id, 
		src.source_table, 
		src.source_system, 
		src.currency_code, 
		current_date,
		current_date
	FROM (
		  SELECT
		  		cc.currency_id::varchar AS currency_src_id,
		  		'CE_CURRENCIES' AS source_table,
				'BL_3NF' AS source_system,
				cc.currency_code
		  FROM 		
		  		bl_3nf.ce_currencies cc
		  WHERE cc.currency_id <> -1
		  )src
		  WHERE NOT EXISTS (
			SELECT 1
			FROM BL_DM.DIM_CURRENCIES wn
			WHERE wn.CURRENCY_SRC_ID    = src.currency_src_id AND 
		  		  wn.CURRENCY_SOURCE_TABLE = src.source_table AND 
		  		  wn.CURRENCY_SOURCE_SYSTEM = src.source_system 
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

	RAISE NOTICE 'ETL: dim_currencies data loaded successfully';		
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


---- Promotions
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_dm_dim_promotions()
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
	v_name := 'bl_cl.proc_insert_bl_dm_dim_promotions';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block
	
	-- main INSERT statement
	INSERT INTO BL_DM.DIM_PROMOTIONS(
			PROMOTION_SURR_ID, PROMOTION_SRC_ID, 
			PROMOTION_SOURCE_TABLE, PROMOTION_SOURCE_SYSTEM, 
			PROMOTION_NAME, PROMOTION_DESCRIPTION, 
			PROMOTION_FEE_DISCOUNT_PERCENT, INSERT_DT, UPDATE_DT
			)
	SELECT
		nextval('bl_dm.dim_promotions_promotion_surr_id_seq'::regclass),
		src.promotion_src_id, 
		src.source_table, 
		src.source_system, 
		src.promotion_name, 
		src.promotion_description, 
		src.promotion_fee_discount_percent, 
		current_date,
		current_date
	FROM (
		  SELECT
		  		p.promotion_id::varchar AS promotion_src_id,
		  		'CE_' AS source_table,
				'BL_3NF' AS source_system,
				p.promotion_name,
				p.promotion_description,
				p.promotion_fee_discount_percent
		  FROM 		
		  		bl_3nf.ce_promotions p
		  WHERE p.promotion_id <> -1
		  )src
		  WHERE NOT EXISTS (
			SELECT 1
			FROM BL_DM.DIM_PROMOTIONS wn
			WHERE wn.PROMOTION_SRC_ID    = src.promotion_src_id AND 
		  		  wn.PROMOTION_SOURCE_TABLE = src.source_table AND 
		  		  wn.PROMOTION_SOURCE_SYSTEM = src.source_system 
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
	
	RAISE NOTICE 'ETL: dim_promotions data loaded successfully';		
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


---- Artists
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_dm_dim_artists_scd()
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
	v_name := 'bl_cl.proc_insert_bl_dm_dim_artists_scd';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block
		
	-- SCD2 UPDATE 
	WITH art_gen AS 
		(SELECT artist_id::varchar AS artist_id, 
				artist_genre_id::varchar AS artist_genre_id, 
				artist_source_table,
				artist_source_system 
		FROM bl_3nf.ce_artists_scd 
		WHERE is_active = 'Y' AND 
				artist_id IN (SELECT artist_id FROM bl_3nf.ce_artists_scd WHERE is_active ='N' GROUP BY artist_id)
		GROUP BY artist_id, artist_genre_id, artist_source_table, artist_source_system
		)
	UPDATE BL_DM.DIM_ARTISTS_SCD da 
	SET 
		is_active = 'N',
		end_dt = current_date 
	WHERE EXISTS (SELECT 1 FROM art_gen src
							WHERE da.artist_src_id = src.artist_id
									)
		  AND da.artist_genre_src_id <> (SELECT ag.artist_genre_id
		  								FROM art_gen ag
		  								WHERE da.artist_src_id = ag.artist_id AND
		  									  da.is_active = 'Y');
	
	-- main INSERT statement
	INSERT INTO BL_DM.DIM_ARTISTS_SCD(
			ARTIST_SURR_ID, ARTIST_SRC_ID, ARTIST_SOURCE_TABLE, 
			ARTIST_SOURCE_SYSTEM, ARTIST_NAME, 
			ARTIST_GENRE_SRC_ID, ARTIST_GENRE, 
			START_DT, END_DT, IS_ACTIVE, INSERT_DT
			)
	SELECT
		nextval('bl_dm.dim_artists_scd_artist_surr_id_seq'::regclass),
		src.artist_src_id, 
		src.source_table, 
		src.source_system, 
		src.artist_name, 
		src.artist_genre_src_id, 
		src.artist_genre, 
		src.start_dt, 
		src.end_dt, 
		src.is_active,
		current_date
	FROM (
		  SELECT
		  		a.artist_id::varchar AS artist_src_id,
		  		'CE_ARTISTS_SCD' AS source_table,
				'BL_3NF' AS source_system,
				a.artist_name, 
				g.genre_id::varchar AS artist_genre_src_id,
				g.genre AS artist_genre, 
				a.start_dt, 
				a.end_dt,
				a.is_active
		  FROM 		
		  			bl_3nf.ce_artists_scd a
		  LEFT JOIN bl_3nf.ce_genres g ON a.artist_genre_id = g.genre_id 
		  WHERE a.artist_id <> -1
		  )src
		  WHERE NOT EXISTS (
			SELECT 1
			FROM  BL_DM.DIM_ARTISTS_SCD wn
			WHERE wn.ARTIST_SRC_ID    = src.artist_src_id AND 
		  		  wn.ARTIST_SOURCE_TABLE = src.source_table AND 
		  		  wn.ARTIST_SOURCE_SYSTEM = src.source_system AND
		  		  wn.START_DT = src.start_dt
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

	RAISE NOTICE 'ETL: dim_artists_scd data loaded successfully';		
 -- handle exceptions			
EXCEPTION 
	WHEN SQLSTATE '42P01' THEN
		RAISE NOTICE 'Check relation`s names!';
	
		v_message  := SQLERRM;
		v_sqlstate := SQLSTATE;
		
		CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime, v_name, v_rcount, v_message, v_sqlstate);
			
	WHEN OTHERS THEN
		v_message  := SQLERRM;
		v_sqlstate := SQLSTATE;
		
		CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime, v_name, v_rcount, v_message, v_sqlstate);
		RAISE;	
END;
$$ LANGUAGE plpgsql; 


-- bl_dm.fct_sales_dd Load procedure
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_dm_fct_sales_dd_part_hist()
AS $$
DECLARE 
cur CURSOR FOR SELECT inhrelid::regclass AS child 
						FROM   pg_catalog.pg_inherits
						WHERE  inhparent = 'bl_dm.fct_sales_dd'::regclass;

v_partition_name regclass;
v_year varchar;
v_quarter varchar;
v_part_start date;
v_part_end date;

--log variables
v_datetime timestamptz;
v_name varchar;
v_row_count int;
v_rcount int := 0;
v_message varchar;
v_sqlstate varchar(5); 
BEGIN
	-- assign variables inside in order they be visible for Exception block
	v_datetime := now();
	v_name := 'bl_cl.proc_insert_bl_dm_fct_sales_dd_part_hist';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	OPEN cur;
	
  	LOOP
	  	FETCH NEXT FROM cur INTO v_partition_name;
	  	EXIT WHEN NOT FOUND;
	  
	    SELECT substring(v_partition_name::text from '\d{4}') INTO v_year; 
		SELECT right(v_partition_name::TEXT, 2) INTO v_quarter;
	
		v_part_start := CASE 
							WHEN v_quarter = 'q1' THEN (v_year||'-01-01')::date
							WHEN v_quarter = 'q2' THEN (v_year||'-04-01')::date
							WHEN v_quarter = 'q3' THEN (v_year||'-07-01')::date
							WHEN v_quarter = 'q4' THEN (v_year||'-10-01')::date
						END;
					
		v_part_end  := CASE 
							WHEN v_quarter = 'q1' THEN (v_year||'-04-01')::date
							WHEN v_quarter = 'q2' THEN (v_year||'-07-01')::date
							WHEN v_quarter = 'q3' THEN (v_year||'-10-01')::date
							WHEN v_quarter = 'q4' THEN ( (v_year::int +1)::TEXT 
																||'-01-01')::date
						END;
	
		EXECUTE 'ALTER TABLE bl_dm.fct_sales_dd DETACH PARTITION ' || v_partition_name::text;
	
		EXECUTE '
        WITH new_fct_rows AS (
            SELECT
                cs.sale_id AS sale_src_id,
                ''CE_SALES'' AS sale_source_table,
                ''BL_3NF'' AS sale_source_system
            FROM
                bl_3nf.ce_sales cs
            WHERE
                event_dt >= ' || quote_literal(v_part_start) || ' AND event_dt < ' || quote_literal(v_part_end) || '
            EXCEPT
            SELECT
                sale_src_id::bigint,
                sale_source_table,
                sale_source_system
            FROM
                ' || v_partition_name || '
        )
        INSERT INTO ' || v_partition_name || ' (
            sale_surr_id,
            sale_src_id,
            sale_source_table,
            sale_source_system,
            shipment_surr_id,
            customer_surr_id,
            product_surr_id,
            promotion_surr_id,
            currency_surr_id,
            artist_surr_id,
            event_dt,
            sale_utc,
            fct_amount_paid_currency,
            fct_amount_paid_usd,
            fct_product_price_currency,
            fct_product_type_platform_fee_percent,
            fct_promotion_fee_discount_percent,
            fct_platform_revenue_usd,
            fct_artist_revenue_usd,
            insert_dt,
            update_dt
        )
        SELECT
            nextval(''bl_dm.fct_sales_dd_sale_surr_id_seq''::regclass),
            cs.sale_id::varchar AS sale_src_id,
            ''CE_SALES'' AS sale_source_table,
            ''BL_3NF'' AS sale_source_system,
            COALESCE(ship.shipment_surr_id, -1),
            COALESCE(cust.customer_surr_id, -1),
            COALESCE(prod.product_surr_id, -1),
            COALESCE(promo.promotion_surr_id, -1),
            COALESCE(curr.currency_surr_id, -1),
            COALESCE(a.artist_surr_id, -1),
            cs.event_dt,
            cs.sale_utc,
            cs.sale_amount_paid AS fct_amount_paid_currency,
            cs.sale_amount_paid_usd AS fct_amount_paid_usd,
            cs.sale_product_price AS fct_product_price_currency,
            cs.sale_product_type_platform_fee_percent AS fct_product_type_platform_fee_percent,
            cs.sale_promotion_fee_discount_percent AS fct_promotion_fee_discount_percent,
            cs.sale_amount_paid_usd * cs.sale_product_type_platform_fee_percent / 100 *
                (1 - cs.sale_promotion_fee_discount_percent/100) AS fct_platform_revenue_usd,
            cs.sale_amount_paid_usd * (1 - cs.sale_product_type_platform_fee_percent / 100) *
                (1 - cs.sale_promotion_fee_discount_percent / 100) AS fct_artist_revenue_usd,
            current_date,
            current_date
        FROM
            bl_3nf.ce_sales cs
        INNER JOIN
            new_fct_rows n ON cs.sale_id = n.sale_src_id
        LEFT JOIN
            bl_dm.dim_shipments ship ON cs.shipment_id::varchar = ship.shipment_src_id
        LEFT JOIN
            bl_dm.dim_customers cust ON cs.customer_id::varchar = cust.customer_src_id
        LEFT JOIN
            bl_dm.dim_products prod ON cs.product_id::varchar = prod.product_src_id
        LEFT JOIN
            bl_dm.dim_promotions promo ON cs.promotion_id::varchar = promo.promotion_src_id
        LEFT JOIN
            bl_dm.dim_currencies curr ON cs.currency_id::varchar = curr.currency_src_id
        LEFT JOIN
            bl_dm.dim_artists_scd a ON cs.artist_id::varchar = a.artist_src_id
        WHERE
            a.is_active = ''Y''';
          
           -- save count of rows affected	
        GET DIAGNOSTICS v_row_count := ROW_COUNT;
        v_rcount := v_rcount + COALESCE(v_row_count, 0);
	
		RAISE NOTICE 'partition: %, new rows %, total rows %', v_partition_name, v_row_count, v_rcount;
		
		EXECUTE 'ALTER TABLE bl_dm.fct_sales_dd ATTACH PARTITION ' 
				|| v_partition_name::text || ' FOR VALUES FROM (' 
       			|| quote_literal(v_part_start) || ') TO (' 
       			|| quote_literal(v_part_end) || ')';
	END LOOP;
	CLOSE cur;
	
	-- provide logging
	IF v_rcount > 0 THEN
		v_message := 'Insert Successful';
	ELSE
		v_message := 'No New Rows Inserted';
	END IF;

	CALL bl_cl.proc_insert_mta_log_etl(
				v_datetime,	v_name,	v_rcount, v_message, v_sqlstate);

	RAISE NOTICE 'ETL: fct_sales_dd data loaded successfully';	
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