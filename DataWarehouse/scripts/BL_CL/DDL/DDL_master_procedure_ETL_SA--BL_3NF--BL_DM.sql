/*
   Create master procedure for Load data:
   				SA layer --> BL_3NF layer --> BL_DM layer.
   Run once.
 */

CREATE OR REPLACE PROCEDURE bl_cl.proc_load_data_to_dwh()
AS $master$
BEGIN
		
	--Load data to BL_3NF---------------------------------------------------------------------
		
		---- Shipments 
		CALL  bl_cl.proc_insert_bl_3nf_ce_shipments();
	
		----Regions
		CALL bl_cl.proc_insert_bl_3nf_ce_regions();
		
		---- Countries
		CALL bl_cl.proc_insert_bl_3nf_ce_countries();
		
		---- Addresses
		CALL bl_cl.proc_insert_bl_3nf_ce_addresses();
		
		---- Customers
		CALL bl_cl.proc_insert_bl_3nf_ce_customers();
		
		---- Product-types
		CALL bl_cl.proc_insert_bl_3nf_ce_product_types();
	
		---- Currencies
		CALL bl_cl.proc_insert_bl_3nf_ce_currencies();
		
		---- Products 
		CALL bl_cl.proc_insert_bl_3nf_ce_products();
	
		---- Genres
		CALL bl_cl.proc_insert_bl_3nf_ce_genres();
				
		---- Artists
		CALL bl_cl.proc_insert_bl_3nf_ce_artists_scd();
	
		---- Promotions
		CALL bl_cl.proc_insert_bl_3nf_ce_promotions();
		
		----CE_SALES
		CALL bl_cl.proc_incremetal_insert_bl_3nf_ce_sales();
		
				
	-- Load to BL_DM-------------------------------------------------------------------------------
		
		---- Customers
		CALL bl_cl.proc_insert_bl_dm_dim_customers();				
				
		---- Shipments
		CALL bl_cl.proc_insert_bl_dm_dim_shipments();
	
		---- Products
		CALL bl_cl.proc_insert_bl_dm_dim_products();
				
		---- Currencies
		CALL bl_cl.proc_insert_bl_dm_dim_currencies();	
	
		---- Artists. SCD2 type.
		CALL bl_cl.proc_insert_bl_dm_dim_artists_scd();	
				
		---- Promotions
		CALL bl_cl.proc_insert_bl_dm_dim_promotions();
			
		----Fact table 
		CALL bl_cl.proc_insert_bl_dm_fct_sales_dd_part_hist();
				
END;
$master$ LANGUAGE plpgsql;