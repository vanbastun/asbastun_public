/*
   Run one time.
   Initial DML insert:
   		- Insert default rows to bl_3nf descriptive tables.
   		- Insert start of time to auxiliary table for incremental load BL_3NF event table.
   		- Insert default rows to bl_dm dimensional tables.
   		- Populate DIM_TIME table.
 */

-----Default rows BL_3NF
CALL bl_3nf.proc_insert_bl_3nf_default_rows();

----MTA table for incremental load
INSERT INTO bl_cl.MTA_BL_3NF_CE_SALES_INCREMENTAL_LOAD(
		source_system, source_table, target_table, 
		procedure_name, previous_loaded_dt)
VALUES ('SA_BANDCAMP_DIGITAL_SALES', 'SRC_BANDCAMP_DIGITAL_SALES', 'CE_SALES', 
		'bl_cl.proc_incremetal_insert_bl_3nf_ce_sales', '1970-01-01 00:00'::timestamp),
		('SA_BANDCAMP_MERCH_SALES', 'SRC_BANDCAMP_MERCH_SALES', 'CE_SALES', 
		'bl_cl.proc_incremetal_insert_bl_3nf_ce_sales', '1970-01-01 00:00'::timestamp);
	

---- Default rows BL_DM
CALL bl_dm.proc_insert_bl_dm_default_rows();

----DIM_TIME insert FROM 1970 to 2030
CALL bl_dm.proc_insert_bl_dm_dim_time_dd();

COMMIT;
