class SqlQueries : 

	# CREATE stmts

	# Staging tables 
	
	flights_staging_create = """ CREATE TABLE IF NOT EXISTS flights_staging (
		callsign varchar,	
		\"number\" varchar, 	
		icao24 varchar,	
		registration varchar, 	
		typecode varchar,	
		origin varchar,	
		destination varchar,	
		firstseen varchar,	
		lastseen varchar,	
		day varchar,	
		latitude_1 float, 	
		longitude_1 float,	
		altitude_1 float,
		latitude_2 float,	
		longitude_2 float,	
		altitude_2 float
	)"""

	tweets_staging_create = """ CREATE TABLE IF NOT EXISTS tweets_staging ( 
		tweet_id bigint, 	
		user_id bigint,	
		\"date\" timestamp, 	
		keywords varchar,	
		location varchar 
	) """

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

