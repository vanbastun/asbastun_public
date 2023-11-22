 
-- 1. target tables don’t contain duplicates.
CALL bl_cl.proc_check_duplicates();

-- 2.1 check if all unique rows from SA layer presented in bl_3nf layer
DO $$
DECLARE
  missing_records INTEGER;
BEGIN
	SELECT count(*) INTO missing_records 
	FROM (	SELECT _id, 
					'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
					'SA_BANDCAMP_DIGITAL_SALES' AS source_system 
			FROM sa_bandcamp_digital_sales.src_bandcamp_digital_sales 
			UNION ALL 
			SELECT _id, 
				'SRC_BANDCAMP_MERCH_SALES',
				'SA_BANDCAMP_MERCH_SALES'  
			FROM sa_bandcamp_merch_sales.src_bandcamp_merch_sales
			EXCEPT 
			SELECT sale_src_id,
					upper(sale_source_table),
					upper(sale_source_system)
			FROM bl_3nf.ce_sales
		  )t;	
	
	IF missing_records = 0 THEN
		RAISE NOTICE 'All records from the SA layer are represented in the bl_3nf layer.';
	ELSE
		RAISE NOTICE 'There are % missing records in the bl_3nf layer.', missing_records;
  	END IF;
END $$;  

--2.2 check if all unique rows from bl_3nf layer presented in bl_dm layer
DO $$
DECLARE
  missing_records INTEGER;
BEGIN
	SELECT count(*) INTO missing_records 
	FROM (	SELECT sale_id FROM bl_3nf.ce_sales
			EXCEPT 
			SELECT sale_src_id::int8 FROM bl_dm.fct_sales_dd
		  )t;	
	
	IF missing_records = 0 THEN
		RAISE NOTICE 'All records from the bl_3nf layer are represented in the bl_dm layer.';
	ELSE
		RAISE NOTICE 'There are % missing records in the bl_dm layer.', missing_records;
  	END IF;
END $$;  



-- Procedure check duplicates
CREATE OR REPLACE PROCEDURE  bl_cl.proc_check_duplicates()
AS $$
BEGIN 
	IF EXISTS 
		(SELECT shipment_src_id, shipment_source_table, shipment_source_system  
		FROM bl_3nf.ce_shipments 
		GROUP BY shipment_src_id, shipment_source_table, shipment_source_system
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_shipments!';
	END IF;	

	IF EXISTS 
		(SELECT address_src_id, address_source_table, address_source_system  
		FROM bl_3nf.ce_addresses 
		GROUP BY address_src_id, address_source_table, address_source_system 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_addresses!';
	END IF;	

	IF EXISTS 
		(SELECT artist_src_id, artist_source_table, artist_source_system  
		FROM bl_3nf.ce_artists_scd 
		WHERE is_active = 'Y'
		GROUP BY artist_src_id, artist_source_table, artist_source_system 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_artists_scd!';
	END IF;	

	IF EXISTS 
		(SELECT country_src_id, country_source_table, country_source_system  
		FROM bl_3nf.ce_countries 
		GROUP BY country_src_id, country_source_table, country_source_system
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_countries!';
	END IF;	

	IF EXISTS 
		(SELECT currency_src_id, currency_source_table, currency_source_system  
		FROM bl_3nf.ce_currencies 
		GROUP BY currency_src_id, currency_source_table, currency_source_system  
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_currencies!';
	END IF;	

	IF EXISTS 
		(SELECT customer_src_id , customer_source_table , customer_source_system 
		FROM bl_3nf.ce_customers 
		GROUP BY customer_src_id , customer_source_table , customer_source_system  
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_customers!';
	END IF;	

	IF EXISTS 
		(SELECT genre_src_id, genre_source_table, genre_source_system  
		FROM bl_3nf.ce_genres
		GROUP BY genre_src_id, genre_source_table, genre_source_system 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_genres!';
	END IF;	

	IF EXISTS 
		(SELECT product_type_src_id, product_type_source_table, product_type_source_system  
		FROM bl_3nf.ce_product_types
		GROUP BY product_type_src_id, product_type_source_table, product_type_source_system  
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_product_types!';
	END IF;	

	IF EXISTS 
		(SELECT product_src_id, product_source_table, product_source_system  
		FROM bl_3nf.ce_products
		GROUP BY product_src_id, product_source_table, product_source_system 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_products!';
	END IF;	

	IF EXISTS 
		(SELECT promotion_src_id, promotion_source_table, promotion_source_system  
		FROM bl_3nf.ce_promotions 
		GROUP BY promotion_src_id, promotion_source_table, promotion_source_system 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_promotions!';
	END IF;	

	IF EXISTS 
		(SELECT region_src_id, region_source_table, region_source_system  
		FROM bl_3nf.ce_regions 
		GROUP BY region_src_id, region_source_table, region_source_system 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_regions!';
	END IF;	

	IF EXISTS 
		(SELECT shipment_src_id, shipment_source_table, shipment_source_system  
		FROM bl_3nf.ce_shipments
		GROUP BY shipment_src_id, shipment_source_table, shipment_source_system 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_shipments!';
	END IF;	

	IF EXISTS 
		(SELECT sale_src_id, sale_source_table, sale_source_system  
		FROM bl_3nf.ce_sales 
		GROUP BY sale_src_id, sale_source_table, sale_source_system 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in ce_sales!';
	END IF;	

	IF EXISTS 
		(SELECT artist_src_id 
		FROM bl_dm.dim_artists_scd 
		WHERE is_active = 'Y'
		GROUP BY artist_src_id 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in dim_artists_scd!';
	END IF;

	IF EXISTS 
		(SELECT currency_src_id 
		FROM bl_dm.dim_currencies 
		GROUP BY currency_src_id
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in dim_currencies!';
	END IF;

	IF EXISTS 
		(SELECT customer_src_id 
		FROM bl_dm.dim_customers 
		GROUP BY customer_src_id 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in dim_customers!';
	END IF;

	IF EXISTS 
		(SELECT  product_src_id 
		FROM bl_dm.dim_products 
		GROUP BY product_src_id 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in dim_products!';
	END IF;

	IF EXISTS 
		(SELECT  promotion_src_id 
		FROM bl_dm.dim_promotions  
		GROUP BY promotion_src_id 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in dim_promotions!';
	END IF;

	IF EXISTS 
		(SELECT  shipment_src_id 
		FROM bl_dm.dim_shipments
		GROUP BY shipment_src_id 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in dim_shipments!';
	END IF;

	IF EXISTS 
		(SELECT sale_src_id 
		FROM bl_dm.fct_sales_dd
		GROUP BY sale_src_id 
		HAVING count(*) > 1)
	THEN 
		RAISE NOTICE 'Duplicates found in fct_sales_dd!';
	END IF;

	RAISE NOTICE 'Duplication check finished. No duplicates was found';
END;
$$ LANGUAGE plpgsql;

CALL bl_cl.proc_check_duplicates();

COMMIT;