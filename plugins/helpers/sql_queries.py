class SqlQueries :
	"""
	Helper class to manage SQL statements to execute during ETL.
	Includes CREATE, INSERT statements for staging, dimension and fact tables.
	"""
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

	# DIMENSION tables
	date_create = """CREATE OR REPLACE VIEW v_dim_date as
		(SELECT
		  	TO_CHAR(datum, 'YYYYMMDD')::INTEGER								AS date_id , 
		    datum::date 													AS full_date,
		    cast(extract(YEAR FROM datum) AS integer)                    	AS year_number,
		    cast(extract(WEEK FROM datum) AS integer)                    	AS year_week_number,
		    cast(extract(DOY FROM datum) AS integer)                     	AS year_day_number,
		    cast(to_char(datum, 'Q') AS integer)                         	AS qtr_number,
		    cast(extract(MONTH FROM datum) AS integer)                   	AS month_number,
		    to_char(datum, 'Month')                                       	AS month_name,
		    cast(to_char(datum, 'D') AS integer)                         	AS week_day_number,-- DAY
		    to_char(datum, 'Day')                                         	AS day_name,
		    CASE WHEN to_char(datum, 'D') IN ('1', '7')
		      THEN 0
		    ELSE 1 END                                                    	AS day_is_weekday,
		    CASE WHEN
		      extract(DAY FROM (datum + (1 - extract(DAY FROM datum)) :: INTEGER +
		                        INTERVAL '1' MONTH) :: DATE -
		                       INTERVAL '1' DAY) = extract(DAY FROM datum)
		      THEN 1
		    ELSE 0 END                                                    	AS day_is_last_of_month
		  FROM
		    -- Generate days for 15 years starting from 2018
		    (
		      SELECT
		        '2018-01-01' :: DATE + generate_series AS datum,
		        generate_series                        AS seq
		      FROM generate_series(0, 15 * 365, 1)
		    ) DQ
		  ORDER BY 1)
	"""

	countries_create = """CREATE TABLE IF NOT EXISTS dim_country (
		id INT identity(1,1),
		name VARCHAR ,
		iso2 varchar(2),
		iso3 varchar(3) 
	)"""

	airports_create = """ CREATE TABLE IF NOT EXISTS dim_airport (
		id int identity(1,1) not null ,
		ref_id integer, 
		code varchar(15) not null,
		type varchar(25),
		name varchar(150),
		municipality varchar(150),
		country_iso2 varchar(2)
	)	
	"""

	airlines_create = """ CREATE TABLE IF NOT EXISTS dim_airline (
		id int identity(1,1) not null ,
		code varchar(10) not null,
		name varchar(150)
	)	
	"""

	# FACT tables

	arrivals_per_airport_create = """
		CREATE TABLE IF NOT EXISTS f_airportarrivals (
			fk_date INTEGER NOT NULL,
			country_iso2 varchar(2),
			airport_code varchar(10), 
			nr_arrivals integer NOT NULL
		)
	"""

	covid_per_country_create = """
		CREATE TABLE IF NOT EXISTS f_countrycovid ( 
			fk_date INTEGER NOT NULL, 
			country_iso2 varchar(2) NOT NULL, 
			new_cases INTEGER , 
			cumulative_cases INTEGER, 
			new_deaths INTEGER, 
			cumulative_deaths INTEGER, 
			people_vaccinated INTEGER, 
			people_vaccinated_per_hundred FLOAT, 
			people_fully_vaccinated INTEGER, 
			people_fully_vaccinated_per_hundred FLOAT
		) 
	"""

	airline_flights_create = """ CREATE TABLE IF NOT EXISTS f_airlineflights ( 
			fk_date INTEGER NOT NULL, 
			airline_code varchar(10),  
			arrival_code varchar(10), 
			departure_code varchar(10),
			nr_planes INTEGER
		) 
	"""

	create_sttmts = [
		# staging
		countries_staging_create, vaccination_staging_create, covid_staging_create, airports_staging_create, tweets_staging_create, flights_staging_create,
		# dims
		date_create, countries_create , airports_create , airlines_create ,
		# facts
		arrivals_per_airport_create, covid_per_country_create , airline_flights_create
	]


	# INSERT STTMTS

	# DIMS 
	countries_insert = """
		INSERT INTO dim_country (name, iso2, iso3)
			SELECT DISTINCT c.name as name, c.alpha_2 as iso2, c.alpha_3  as iso3
				FROM countries_staging c 
				WHERE c.alpha_2 is not null 
					AND c.alpha_2 NOT IN ( SELECT DISTINCT alpha_2 from dim_country );
	"""

	airports_insert = """ 
		INSERT INTO dim_airport ( ref_id, code , type, name, municipality, country_iso2 )
			SELECT DISTINCT id as ref_id , code, type, name, municipality , iso_country as country_iso2 
				FROM airports_staging 
				WHERE code IS NOT NULL 
					AND code NOT IN (SELECT code FROM dim_airport) ;
	"""

	airlines_insert = """ 
		INSERT INTO dim_airline ( code, name )
			SELECT DISTINCT REGEXP_SUBSTR ( callsign, '[A-Za-z]{3}') as airline_code, 'TODO: Enrich' as airline_name
				from flights_staging 
				WHERE callsign IS NOT NULL 
					AND airline_code not in (SELECT code FROM dim_airline)  ;
	;"""


	populate_dims_sttmts = [ countries_insert , airports_insert , airlines_insert]


	# FACTS 

	airport_arrivals_insert = """
		INSERT INTO f_airportarrivals ( fk_date, country_iso2, airport_code, nr_arrivals )
			(
				SELECT 
					DISTINCT 
					replace(left(f.arrival_at, 10), '-', '')::INTEGER fk_date,
					a.iso_country as country_iso2,
					a.code as airport_code,
					count(distinct f.trasponder_id) as nr_arrivals 
				FROM flights_staging as f 
				LEFT JOIN airports_staging as a 
					on a.code = f.arrival_airport_id
				WHERE 
					country_iso2 is not null and airport_code is not null and fk_date is not null
				GROUP BY 
					fk_date , country_iso2, airport_code 
			);
	"""

	airline_flights_insert = """  
		INSERT INTO f_airlineflights ( fk_date, airline_code , arrival_code, departure_code, nr_planes )
			( 
				SELECT 
					DISTINCT 
                    replace(left(f.arrival_at, 10), '-', '')::INTEGER fk_date,
					REGEXP_SUBSTR ( f.callsign, '[A-Za-z]{3}') as airline_code,
					arrival_airport_id as arrival_code,
					depart_airport_id as departure_code,
					count(distinct f.trasponder_id) as nr_planes 
				FROM flights_staging as f 
				WHERE 
					fk_date is not null and airline_code is not null
				GROUP BY 
					fk_date , arrival_code, airline_code, departure_code
			)
			;
	"""

	covid_per_country_insert = """
		INSERT INTO f_countrycovid ( fk_date, country_iso2 , new_cases  , cumulative_cases , new_deaths , cumulative_deaths , people_vaccinated , people_vaccinated_per_hundred, people_fully_vaccinated , people_fully_vaccinated_per_hundred )
				( WITH covid as (
					SELECT 	cov.date_reported,
							cov.new_cases, cov.cumulative_cases, cov.new_deaths, cov.cumulative_deaths, 
							c.alpha_2 , c.alpha_3 
						FROM covid_staging cov 
						LEFT JOIN countries_staging c 
							ON c.alpha_2 = cov.country_code 
						WHERE 
							cov.cumulative_cases > 0
							AND c.alpha_2 IS NOT NULL 
				) , vaccination as (				
					SELECT 	vac."date",
						vac.people_vaccinated,  
						vac.people_vaccinated_per_hundred,  
						vac.people_fully_vaccinated, 
						vac.people_fully_vaccinated_per_hundred, 
						c.alpha_2 , c.alpha_3 
					FROM vaccination_staging vac 
					LEFT JOIN countries_staging c
						ON c.alpha_3 = vac.iso_code
				) 
				SELECT 	
						replace(coalesce( c.date_reported, v.date ), '-', '')::INTEGER as fk_date 
						,coalesce(c.alpha_2, v.alpha_2) as country_iso2 ,
						c.new_cases, 
						c.cumulative_cases, 
						c.new_deaths, 
						c.cumulative_deaths,
						v.people_vaccinated,  
						v.people_vaccinated_per_hundred,  
						v.people_fully_vaccinated, 
						v.people_fully_vaccinated_per_hundred
					FROM covid c
					LEFT JOIN vaccination v
						ON v.date = c.date_reported and c.alpha_2 = v.alpha_2
					WHERE
 						fk_date is not null and country_iso2 is not null	
					ORDER BY fk_date DESC 
				)
	"""

	populate_facts_sttmts = [ airport_arrivals_insert , airline_flights_insert , covid_per_country_insert ]



 