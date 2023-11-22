/* ETL:
   		Files --> SA layer --> BL_3NF layer --> BL_DM layer. 
 
 	* Procedures for ETL process when we are getting sources files with different names. 
 	  !!!! check path to files !!!!  */
 

--New data files will be readed and loaded to source SA layer 

----1. read source file with digital sales data
CALL bl_cl.proc_create_ext_digital('D:\\dwh\\bandcamp_digital_sales5.csv');

----2. read source file with merch sales data
CALL bl_cl.proc_create_ext_merch('D:\\dwh\\bandcamp_merch_sales5.csv');

----3. full reload sources tables from external tables.
CALL bl_cl.proc_full_reload_source_tables();


-- Load data:	SA layer --> BL_3NF layer --> BL_DM layer.
CALL bl_cl.proc_load_data_to_dwh();