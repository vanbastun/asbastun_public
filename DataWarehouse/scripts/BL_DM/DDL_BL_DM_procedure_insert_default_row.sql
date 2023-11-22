/*
   Create procedure for insert default rows into BL_DM tables.   
   Run once.
 */

---- DEFAULT rows INSERT to all BL_DM dimensional  tables
CREATE OR REPLACE PROCEDURE bl_dm.proc_insert_bl_dm_default_rows()
AS $$
BEGIN 
		--customers
		INSERT INTO BL_DM.DIM_CUSTOMERS (
				CUSTOMER_SURR_ID,
				CUSTOMER_SRC_ID,
				CUSTOMER_SOURCE_TABLE,
				CUSTOMER_SOURCE_SYSTEM,
				CUSTOMER_NAME,
				CUSTOMER_GENDER,
				CUSTOMER_EMAIL,
				CUSTOMER_BIRTHDATE_DT,
				CUSTOMER_ADDRESS_SRC_ID,
				CUSTOMER_ADDRESS_TEXT,
				CUSTOMER_ADDRESS_POSTCODE,
				CUSTOMER_COUNTRY_SRC_ID,
				CUSTOMER_COUNTRY_NAME,
				CUSTOMER_COUNTRY_CODE,
				CUSTOMER_REGION_SRC_ID,
				CUSTOMER_REGION_NAME,
				INSERT_DT,
				UPDATE_DT		
				)
		SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', 'n.a.', 'n.a.', '1900-01-01'::date, 
					'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 
					'1900-01-01'::date, '1900-01-01'::date
		WHERE NOT EXISTS (
		    				SELECT 1
		    				FROM BL_DM.DIM_CUSTOMERS
		    				WHERE CUSTOMER_SURR_ID = -1
		    			);
				
		--products
		INSERT INTO BL_DM.DIM_PRODUCTS (
				PRODUCT_SURR_ID,		
				PRODUCT_SRC_ID,		
				PRODUCT_SOURCE_TABLE,	
				PRODUCT_SOURCE_SYSTEM,	
				PRODUCT_URL,		
				PRODUCT_DESCRIPTION,	
				PRODUCT_IMAGE_URL,	
				PRODUCT_ALBUM_TITLE,	
				PRODUCT_TYPE_SRC_ID,
				PRODUCT_TYPE_CODE,
				INSERT_DT,		
				UPDATE_DT
				)
		SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 
					'n.a.', 'n.a.', '1900-01-01'::date, '1900-01-01'::date
		WHERE NOT EXISTS (
		    				SELECT 1
		    				FROM BL_DM.DIM_PRODUCTS
		    				WHERE PRODUCT_SURR_ID = -1
		    			); 
		    			
		--shipments
		INSERT INTO  BL_DM.DIM_SHIPMENTS(
				SHIPMENT_SURR_ID,		
				SHIPMENT_SRC_ID,	
				SHIPMENT_SOURCE_TABLE,	
				SHIPMENT_SOURCE_SYSTEM,	
				SHIPMENT_TRACK_NUMBER,	
				SHIPMENT_DT,		
				SHIPMENT_RECEIVED_DT,	
				INSERT_DT,		
				UPDATE_DT
				)
		SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-01-01'::date, 
				'9999-12-31'::date, '1900-01-01'::date, '1900-01-01'::date
		WHERE NOT EXISTS (
		    				SELECT 1
		    				FROM BL_DM.DIM_SHIPMENTS
		    				WHERE SHIPMENT_SURR_ID = -1
		    			);  
		    			
		--currencies
		INSERT INTO BL_DM.DIM_CURRENCIES (
				CURRENCY_SURR_ID,		
				CURRENCY_SRC_ID,	
				CURRENCY_SOURCE_TABLE,	
				CURRENCY_SOURCE_SYSTEM,	
				CURRENCY_CODE,		
				INSERT_DT,		
				UPDATE_DT
				)
		SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-01-01'::date, '1900-01-01'::date
		WHERE NOT EXISTS (
		    				SELECT 1
		    				FROM BL_DM.DIM_CURRENCIES
		    				WHERE CURRENCY_SURR_ID = -1
		    			);
		
		--promotions
		INSERT INTO BL_DM.DIM_PROMOTIONS (
				PROMOTION_SURR_ID,		
				PROMOTION_SRC_ID,	
				PROMOTION_SOURCE_TABLE,	
				PROMOTION_SOURCE_SYSTEM,	
				PROMOTION_NAME,		
				PROMOTION_DESCRIPTION,	
				PROMOTION_FEE_DISCOUNT_PERCENT,	
				INSERT_DT,		
				UPDATE_DT
				)
		SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', 'n.a.', -1, '1900-01-01'::date, '1900-01-01'::date
		WHERE NOT EXISTS (
		    				SELECT 1
		    				FROM BL_DM.DIM_PROMOTIONS
		    				WHERE PROMOTION_SURR_ID = -1
		    			); 
		    			
		--artists
		INSERT INTO BL_DM.DIM_ARTISTS_SCD (
				ARTIST_SURR_ID,		
				ARTIST_SRC_ID,		
				ARTIST_SOURCE_TABLE,	
				ARTIST_SOURCE_SYSTEM,	
				ARTIST_NAME,		
				ARTIST_GENRE_SRC_ID,
				ARTIST_GENRE,
				START_DT,		
				END_DT,			
				IS_ACTIVE,		
				INSERT_DT
				)
		SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', 'n.a.', 'n.a.', '1900-01-01'::date, 
				'9999-12-31'::date, 'n.a.', '1900-01-01'::date
		WHERE NOT EXISTS (
		    				SELECT 1
		    				FROM BL_DM.DIM_ARTISTS_SCD
		    				WHERE ARTIST_SURR_ID = -1
		    			);
-- handle exceptions			
EXCEPTION 
	WHEN SQLSTATE '42P01' THEN
		RAISE NOTICE 'Check relation`s names!';
	
	WHEN OTHERS THEN
		RAISE;		
END;
$$ LANGUAGE plpgsql;		    			
