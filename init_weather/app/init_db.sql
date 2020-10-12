create schema if not exists "weather";

create table if not exists "weather"."wind"(
		id_ varchar,
		hour_ int,
		date_ date,
		wind_speed_ms float,
		UNIQUE(id_, hour_, date_)
);

create table if not exists "weather"."temperature"(
		id_ varchar,
		hour_ int,
		date_ date,
		air_temperatur_celsius float,
		UNIQUE(id_, hour_, date_)
);

create table if not exists "weather"."precipitation"(
		id_ varchar,
		date_ date,
		precipitation_mm float,
		UNIQUE(id_, date_)
);

create table if not exists "weather"."wind_stations"(
		id_ varchar primary key, 
		name_ varchar, 
		lat_ float, 
		lon_ float
		);

create table if not exists "weather"."prec_temp_stations"(
		id_ varchar primary key, 
		name_ varchar, 
		lat_ float, 
		lon_ float
		);