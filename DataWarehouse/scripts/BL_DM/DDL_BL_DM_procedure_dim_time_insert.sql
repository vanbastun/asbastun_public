/*
   Create procedure for Load data INTO DIM_TIME table.
   Run once.
 */

--DIM_TIME insert FROM 1970 to 2030
CREATE OR REPLACE PROCEDURE bl_dm.proc_insert_bl_dm_dim_time_dd()
AS $$
BEGIN 
	INSERT INTO BL_DM.DIM_TIME_DD
	SELECT
		datum AS EVENT_DT,
		extract(year FROM datum) AS DIM_TIME_YEAR,
		extract(month FROM datum) AS DIM_TIME_MM,
		-- Localized month name
		to_char(datum, 'TMMonth') AS DIM_TIME_MONTH_NAME,
		extract(day FROM datum) AS DIM_TIME_DAY,
		extract(doy FROM datum) AS DIM_TIME_DAY_OF_YEAR,
		-- Localized weekday
		to_char(datum, 'TMDay') AS DIM_TIME_WEEKDAY_NAME,
		-- ISO calendar week
		extract(week FROM datum) AS DIM_TIME_CALENDAR_WEEK_NO,
		to_char(datum, 'dd. mm. yyyy') AS DIM_TIME_FORMATTED_DATE,
		'Q' || to_char(datum, 'Q') AS DIM_TIME_QUARTER,
		to_char(datum, 'yyyy/"Q"Q') AS DIM_TIME_YEAR_QUARTER,
		to_char(datum, 'yyyy/mm') AS DIM_TIME_YEAR_MONTH,
		-- ISO calendar year and week
		to_char(datum, 'iyyy/IW') AS DIM_TIME_YEAR_CALENDAR_WEEK,
		-- Weekend
		CASE WHEN extract(isodow FROM datum) in (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS DIM_TIME_WEEKEND,
		-- Some fixed holidays for America (bandacmp founded in the USA)
	    CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0704', '1225', '1226')
			THEN 'Holiday' ELSE 'No holiday' END
			AS DIM_TIME_USA_HOLIDAY,
		-- ISO start and end of the week of this date
		datum + (1 - extract(isodow FROM datum))::integer AS DIM_TIME_CURRENT_WEEK_STARTDAY,
		datum + (7 - extract(isodow FROM datum))::integer AS DIM_TIME_CURRENT_WEEK_ENDDAY,
		-- Start and end of the month of this date
		datum + (1 - extract(day FROM datum))::integer AS DIM_TIME_CURRENT_MONTH_STARTDAY,
		((datum + (1 - extract(day FROM datum))::integer + '1 month'::interval) - '1 day'::interval)::date AS DIM_TIME_CURRENT_MONTH_ENDDAY
	FROM (
		-- There are 61 year between 1970 and 2030, and 15 leap years in this range, so calculate 365 * 61 + 15 records = 22280 days
		SELECT '1970-01-01'::DATE + sequence.day AS datum
		FROM generate_series(0, 22280) AS sequence(day)
		GROUP BY sequence.day
	     ) DQ
	;
-- handle exceptions			
EXCEPTION 
	WHEN SQLSTATE '42P01' THEN
		RAISE NOTICE 'Check relation`s names!';
	
	WHEN OTHERS THEN
		RAISE;		
END;
$$ LANGUAGE plpgsql;	