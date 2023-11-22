/* DDL code to create next objects:
	  1. Function to grant all privileges to role on all schemas (for db_developer)
	  2. Logging functionality: table, procedure
	  3. Work table and procedure to fill it, for SCD2 table ETL
	  4. Table to prepare incremental load bl_3nf fact table
 */

----Grant privileges function.
------Calling result: grant all privileges to role on all schemas
CREATE OR REPLACE FUNCTION bl_cl.func_grant_all_privileges_on_database_to_role(
    	role_name text,
    	database_name text
		)
RETURNS void
LANGUAGE plpgsql AS
$function$
DECLARE
    rec RECORD;
BEGIN
    EXECUTE FORMAT('GRANT ALL ON DATABASE %1$s TO %2$s', database_name, role_name);

    FOR rec IN (
        SELECT nspname AS name
        FROM   pg_namespace
        WHERE  nspname NOT IN (      -- EXCLUDE SYSTEM SCHEMAS 
                'information_schema',
        		'pg_catalog',
        		'pg_toast',
        		'pg_temp_1',
        		'pg_toast_temp_1',
        		'pg_toast_temp_2'
            )
    ) LOOP
        EXECUTE FORMAT('GRANT ALL ON SCHEMA %1$s TO %2$s', rec.name, role_name);
        EXECUTE FORMAT('GRANT ALL ON ALL TABLES IN SCHEMA %1$s TO %2$s', rec.name, role_name);
        EXECUTE FORMAT('GRANT ALL ON ALL SEQUENCES IN SCHEMA %1$s TO %2$s', rec.name, role_name);
        EXECUTE FORMAT('GRANT ALL ON ALL FUNCTIONS IN SCHEMA %1$s TO %2$s', rec.name, role_name);
        EXECUTE FORMAT('GRANT ALL ON ALL PROCEDURES IN SCHEMA %1$s TO %2$s', rec.name, role_name);
        EXECUTE FORMAT('GRANT ALL ON ALL ROUTINES IN SCHEMA %1$s TO %2$s', rec.name, role_name);
    END LOOP;
END;
$function$;


-------------------- Logging functionality
-- logging table
CREATE TABLE IF NOT EXISTS BL_CL.MTA_LOG_ETL(
	datetime timestamptz,
	procedure_name varchar(255),
	number_of_rows_affected int,
	message varchar(4096), --default length of error message in PostgreSQL
	sqlstate_code varchar(5)
);

-- logging procedure			
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_mta_log_etl(
				INOUT datetime timestamptz,
				INOUT pr_name varchar,
				INOUT rows_count int,
				INOUT message varchar,
				INOUT sqlstate_code varchar(5)
				) 
AS $$
BEGIN 
	INSERT INTO BL_CL.MTA_LOG_ETL (
			datetime, procedure_name,
			number_of_rows_affected, message, sqlstate_code)
	VALUES (datetime, pr_name, rows_count, message, sqlstate_code);	

EXCEPTION 
	WHEN OTHERS
		THEN RAISE;
END;
$$ LANGUAGE plpgsql;


------------------ Preparation to apply SCD2 logic for ETL SA --> BL_3NF

-- create working table from sources to multiple calling it instead whole source table
CREATE TABLE IF NOT EXISTS BL_CL.WRK_ARTISTS_GENRES(
	artist_src_id varchar(50),
	genre_source_id varchar(50),
	source_table varchar(50),
	source_system varchar(50),
	artist_name varchar(150),
	genre varchar(50)
	);
	
-- create working table from sources to multiple calling it instead whole source table
CREATE OR REPLACE PROCEDURE bl_cl.proc_insert_bl_cl_wrk_atrists_genres()
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
	v_name := 'bl.cl_proc_insert_bl_cl_wrk_atrists_genres';
	v_sqlstate := '00000'; --by default 'successful completion' code, may be changed in the exception block

	--reload full data from sources
	EXECUTE 'TRUNCATE TABLE bl_cl.wrk_artists_genres' ;
	
	INSERT INTO bl_cl.wrk_artists_genres (
				artist_src_id, genre_source_id,
				source_table, source_system,
				artist_name, genre	
		)
	SELECT
		src.artist_src_id,
		src.genre,
		src.source_table, 
		src.source_system, 
		src.artist_name,
		src.genre			
	FROM (
		SELECT 
			COALESCE(ds.artist_id, 'n.a.') AS artist_src_id,
			ds.genre,
			'SRC_BANDCAMP_DIGITAL_SALES' AS source_table,
			'SA_BANDCAMP_DIGITAL_SALES' AS source_system,
			COALESCE(ds.artist_name, 'n.a.') AS artist_name
		FROM 
			sa_bandcamp_digital_sales.src_bandcamp_digital_sales ds
		GROUP BY COALESCE(ds.artist_id, 'n.a.'), ds.genre, COALESCE(ds.artist_name, 'n.a.')
		UNION ALL
		SELECT 
			COALESCE(ms.artist_id, 'n.a.') AS artist_src_id,
			ms.genre,
			'SRC_BANDCAMP_MERCH_SALES' AS source_table,
			'SA_BANDCAMP_MERCH_SALES' AS source_system,
			COALESCE(ms.artist_name, 'n.a.') AS artist_name
		FROM 
			sa_bandcamp_merch_sales.src_bandcamp_merch_sales ms
		GROUP BY COALESCE(ms.artist_id, 'n.a.'), ms.genre, COALESCE(ms.artist_name, 'n.a.')
		) src;
	
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


------------------ Preparation to incremental load bl_3nf fact table
CREATE TABLE IF NOT EXISTS bl_cl.MTA_BL_3NF_CE_SALES_INCREMENTAL_LOAD(
			source_system varchar,
			source_table varchar,
			target_table varchar,
			procedure_name varchar,
			previous_loaded_dt timestamp
		);
		
	