class SqlQueries : 

	# CREATE stmts

	# Staging tables 
	
	flights_staging_create = """ CREATE TABLE IF NOT EXISTS flights_staging (
		callsign varchar,
		trasponder_id varchar,	
		aircraft_id varchar, 	
		aircraft_type varchar,	
		depart_airport_id varchar,	
		arrival_airport_id varchar,	
		depart_at varchar,	
		arrival_at varchar
	)"""

	tweets_staging_create = """ CREATE TABLE IF NOT EXISTS tweets_staging ( 
		\"date\" varchar, 	
		keywords varchar,	
		country varchar,
		tweet_id bigint
	) """

	airports_staging_create = """ CREATE TABLE IF NOT EXISTS airports_staging (
		id integer ,
		code varchar(25),
		type varchar(25),
		name varchar(150),
		iso_country varchar(10),
		municipality varchar(150) 
	)
	"""

	covid_staging_create = """ CREATE TABLE IF NOT EXISTS covid_staging (  	
		Date_reported varchar, 	
		Country_code varchar,	
		Country varchar,	
		WHO_region varchar,	
		New_cases bigint,	
		Cumulative_cases bigint,	
		New_deaths bigint,	
		Cumulative_deaths bigint
	) """

	vaccination_staging_create = """ CREATE TABLE IF NOT EXISTS vaccination_staging ( 
		\"date\" varchar, 	
		total_vaccinations float,
		people_vaccinated float,	
		total_vaccinations_per_hundred float, 	
		people_vaccinated_per_hundred float,	
		daily_vaccinations float,	
		daily_vaccinations_per_million float, 	
		daily_vaccinations_raw float,
		people_fully_vaccinated float,	
		people_fully_vaccinated_per_hundred float, 	
		country varchar,
		iso_code varchar
	) """

	countries_staging_create = """ CREATE TABLE IF NOT EXISTS countries_staging( 
		name varchar(150),
		alpha_2 varchar(2),
		alpha_3 varchar(3)
	)
	"""


	create_sttmts = [countries_staging_create, vaccination_staging_create, covid_staging_create, airports_staging_create, tweets_staging_create, flights_staging_create]


	# DIMENSIONS sttmts

	# CREATE dims 
	countries_staging_create = """ CREATE TABLE if not exists date_dimension (
		date_id               INTEGER                     NOT NULL PRIMARY KEY,  -- DATE
		full_date             DATE                        NOT NULL,
		year_number           SMALLINT                    NOT NULL,
		year_week_number      SMALLINT                    NOT NULL,
		year_day_number       SMALLINT                    NOT NULL,
		qtr_number            SMALLINT                    NOT NULL,
		month_number          SMALLINT                    NOT NULL,
		month_name            CHAR(9)                     NOT NULL,
		month_day_number      SMALLINT                    NOT NULL,  -- WEEK
		week_day_number       SMALLINT                    NOT NULL,  -- DAY
		day_name              CHAR(9)                     NOT NULL,
		day_is_weekday        SMALLINT                    NOT NULL,
		day_is_last_of_month  SMALLINT                    NOT NULL
	);
	"""

	date_insert = """
		select
		  	TO_CHAR(datum, 'YYYYMMDD') :: INTEGER						as date_id , 
		    datum                                                         AS full_date,
		    cast(extract(YEAR FROM datum) AS SMALLINT)                    AS year_number,
		    cast(extract(WEEK FROM datum) AS SMALLINT)                    AS year_week_number,
		    cast(extract(DOY FROM datum) AS SMALLINT)                     AS year_day_number,
		    cast(to_char(datum, 'Q') AS SMALLINT)                         AS qtr_number,
		    cast(extract(MONTH FROM datum) AS SMALLINT)                   AS month_number,
		    to_char(datum, 'Month')                                       AS month_name,
		    cast(extract(DAY FROM datum) AS SMALLINT)                     AS month_day_number,-- WEEK
		    cast(to_char(datum, 'D') AS SMALLINT)                         AS week_day_number,-- DAY
		    to_char(datum, 'Day')                                         AS day_name,
		    CASE WHEN to_char(datum, 'D') IN ('1', '7')
		      THEN 0
		    ELSE 1 END                                                    AS day_is_weekday,
		    CASE WHEN
		      extract(DAY FROM (datum + (1 - extract(DAY FROM datum)) :: INTEGER +
		                        INTERVAL '1' MONTH) :: DATE -
		                       INTERVAL '1' DAY) = extract(DAY FROM datum)
		      THEN 1
		    ELSE 0 END                                                    AS day_is_last_of_month
		  FROM
		    -- Generate days for the next ~20 years starting from 2011.
		    (
		      SELECT
		        '2018-01-01' :: DATE + generate_series AS datum,
		        generate_series                        AS seq
		      FROM generate_series(0, 15 * 365, 1)
		    ) DQ
		  ORDER BY 1;
	"""

	