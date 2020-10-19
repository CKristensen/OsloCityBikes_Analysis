
delete from "airflow"."day_bikeTrip"
where start_date = (select cast(to_char(cast(started_at  as date), 'YYYYMMDD') as int) from airflow.day_dump dd group by cast(to_char(cast(started_at  as date), 'YYYYMMDD') as int));

insert into "airflow"."day_bikeTrip"(start_station_id, end_station_id, start_date, start_time, end_date, end_time, duration, air_temperatur_celsius, wind_speed_ms, precipitation_mm)
select start_station_id, 
	end_station_id, 
	cast(to_char(cast(started_at  as date), 'YYYYMMDD') as int) as start_date, 
	cast(to_char(cast(started_at  as time), 'HH24MI') as int) as start_time, 
	cast(to_char(cast(ended_at  as date), 'YYYYMMDD') as int) as end_date, 
	cast(to_char(cast(ended_at  as time), 'HH24MI') as int) as end_time, 
	cast(duration as int),
	air_temperatur_celsius,
	wind_speed_ms,
	precipitation_mm from airflow.day_dump dd;
