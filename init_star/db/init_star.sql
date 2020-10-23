create schema if not exists "star";
create schema if not exists "airflow";

SELECT * FROM bysykkel.elevation;
SELECT * FROM weather.temperature;

/***ADDING STATION DIMENSION **/
create table if not exists "star"."station" (
	station_key varchar primary key,
	station_name varchar,
	description varchar,
	latitude float,
	longitude float
);

alter table "star"."station"
add column station_key_start varchar unique;
alter table "star"."station"
add column station_key_end varchar unique;


/***ADDING TIME DIMENSION **/
CREATE TABLE "star"."time" (
    time_key int4 NOT NULL,
    time time,
    hour int2,
    military_hour int2,
    minute int4,
    second int4,
    minute_of_day int4,
    second_of_day int4,
    quarter_hour varchar,
    am_pm varchar,
    day_night varchar,
    day_night_abbrev varchar,
    time_period varchar,
    time_period_abbrev varchar
)
WITH (OIDS=FALSE);

TRUNCATE TABLE "star"."time";

-- Unknown member
INSERT INTO "star"."time" VALUES (
    -1, --id
    '0:0:0', -- time
    0, -- hour
    0, -- military_hour
    0, -- minute
    0, -- second
    0, -- minute_of_day
    0, -- second_of_day
    'Unknown', -- quarter_hour
    'Unknown', -- am_pm
    'Unknown', -- day_night
    'Unk', -- day_night_abbrev
    'Unknown', -- time_period
    'Unk' -- time_period_abbrev
);

INSERT INTO "star"."time"
SELECT
  to_char(datum, 'HH24MISS')::integer AS time_key,
  datum::time AS time,
  to_char(datum, 'HH12')::integer AS hour,
  to_char(datum, 'HH24')::integer AS military_hour,
  extract(minute FROM datum)::integer AS minute,
  extract(second FROM datum) AS second,
  to_char(datum, 'SSSS')::integer / 60 AS minute_of_day,
  to_char(datum, 'SSSS')::integer AS second_of_day,
  to_char(datum - (extract(minute FROM datum)::integer % 15 || 'minutes')::interval, 'hh24:mi') ||
  ' ? ' ||
  to_char(datum - (extract(minute FROM datum)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, 'hh24:mi')
    AS quarter_hour,
  to_char(datum, 'AM') AS am_pm,
  CASE WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '19:59' THEN 'Day (8AM-8PM)' ELSE 'Night (8PM-8AM)' END
  AS day_night,
  CASE WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '19:59' THEN 'Day' ELSE 'Night' END
  AS day_night_abbrev,
  CASE
  WHEN to_char(datum, 'hh24:mi') BETWEEN '00:00' AND '03:59' THEN 'Late Night (Midnight-4AM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '04:00' AND '07:59' THEN 'Early Morning (4AM-8AM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '11:59' THEN 'Morning (8AM-Noon)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '12:00' AND '15:59' THEN 'Afternoon (Noon-4PM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '16:00' AND '19:59' THEN 'Evening (4PM-8PM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '20:00' AND '23:59' THEN 'Night (8PM-Midnight)'
  END AS time_period,
  CASE
  WHEN to_char(datum, 'hh24:mi') BETWEEN '00:00' AND '03:59' THEN 'Late Night'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '04:00' AND '07:59' THEN 'Early Morning'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '11:59' THEN 'Morning'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '12:00' AND '15:59' THEN 'Afternoon'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '16:00' AND '19:59' THEN 'Evening'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '20:00' AND '23:59' THEN 'Night'
  END AS time_period_abbrev
FROM generate_series('2000-01-01 00:00:00'::timestamp, '2000-01-01 23:59:59'::timestamp, '1 minute') datum;

ALTER TABLE "star"."time" ADD CONSTRAINT times_time_key_pk PRIMARY KEY (time_key);

update "star"."time" set time_key = ROUND(time_key/100, 0) where time_key > 0;

alter table "star"."time"
add column time_key_end int unique;
UPDATE "star"."time"
SET time_key_end = time_key;

alter table "star"."time"
add column time_key_start int unique;
UPDATE "star"."time"
SET time_key_start = time_key;

ALTER TABLE "star"."time" 
DROP COLUMN quarter_hour;

DELETE FROM "star"."time" 
WHERE time_key=-1;


/***ADDING DATE DIMENSION **/

DROP TABLE if exists "star"."date";

CREATE TABLE "star"."date"
(
  date_key                  INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

ALTER TABLE "star"."date" ADD CONSTRAINT dates_date_key_pk PRIMARY KEY (date_key);

CREATE INDEX dates_date_actual_idx
  ON "star"."date"(date_actual);

COMMIT;

INSERT INTO "star"."date"
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_key,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'Day') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'Month') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(ISOYEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '2016-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 2191) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

alter table star.date
add column date_key_start int unique;
UPDATE star.date
SET date_key_start = date_key;

alter table star.date
add column date_key_end int unique;
UPDATE star.date
SET date_key_end = date_key;


create table if not exists "star"."bikeTrip" (
  	s_key serial primary key,
	start_station_id varchar references "star"."station"(station_key_start),
	end_station_id varchar references "star"."station"(station_key_end),
	start_date int references "star"."date"(date_key_start),
	start_time int references "star"."time"(time_key_start),
	end_date int references "star"."date"(date_key_end),
	end_time int references "star"."time"(time_key_end),
	duration int,
	air_temperatur_celsius float,
	wind_speed_ms float,
	precipitation_mm float
	
);

alter table star.station add column elevation float;

insert into star.station 
select station_key, station_name, description, latitude,
longitude, station_key station_key_start, station_key station_key_end, elevation
from bysykkel.elevation;


insert into star.station(station_key, station_key_start, station_key_end, station_name)
select sid, sid, sid, '_LEGACY_' from
(select distinct("Start station") sid from bysykkel.obos_data_legacy odl where "Start station" is not null) legacies 
on conflict do nothing;

create table bysykkel.temp_bike as
select start_station_id, 
	end_station_id, 
	cast(to_char(cast(started_at  as date), 'YYYYMMDD') as int) as start_date, 
	cast(to_char(cast(started_at  as time), 'HH24MI') as int) as start_time, 
	cast(to_char(cast(ended_at  as date), 'YYYYMMDD') as int) as end_date, 
	cast(to_char(cast(ended_at  as time), 'HH24MI') as int) as end_time, 
	EXTRACT(EPOCH FROM (ended_at::timestamp - started_at::timestamp)) duration
	from (select coalesce(start.station_key, odl."Start station") as start_station_id, 
			"Start time" as started_at, 
			"End time" as ended_at, 
			coalesce(end_.station_key, odl."End station") as end_station_id
			from bysykkel.obos_data_legacy odl 
			left join (select station_key, leg_id from bysykkel.elevation) start on odl."Start station" = start.leg_id
			left join (select station_key, leg_id from bysykkel.elevation) end_ on odl."End station" = end_.leg_id) kk;

insert into bysykkel.temp_bike(start_station_id, end_station_id, start_date, start_time, end_date, end_time, duration)
select cast(start_station_id as int),
	cast(end_station_id as int), 
	cast(to_char(cast(started_at  as date), 'YYYYMMDD') as int) as start_date, 
	cast(to_char(cast(started_at  as time), 'HH24MI') as int) as start_time, 
	cast(to_char(cast(ended_at  as date), 'YYYYMMDD') as int) as end_date, 
	cast(to_char(cast(ended_at  as time), 'HH24MI') as int) as end_time, 
	EXTRACT(EPOCH FROM (ended_at::timestamp - started_at::timestamp)) duration
	from bysykkel.obos_data od;
	

create view star.weather as
select cast(to_char(w.date_, 'YYYYMMDD') as int) as date, time_key, air_temperatur_celsius, precipitation_mm, wind_speed_ms
from weather.wind w
join weather.temperature t 
on t.date_ = w.date_ and t.hour_ =w.hour_
join weather.precipitation p on w.date_ = p.date_
join star."time" tt on tt.military_hour=t.hour_
where tt.time_key <> -1 order by w.date_, tt.time_key;

create view star.weather_by_hour as
select cast(to_char(w.date_, 'YYYYMMDD') as int) as date, tt.military_hour, 
max(air_temperatur_celsius) air_temperatur_celsius, 
max(precipitation_mm) precipitation_mm, max(wind_speed_ms) wind_speed_ms
from weather.wind w
join weather.temperature t 
on t.date_ = w.date_ and t.hour_ =w.hour_
join weather.precipitation p on w.date_ = p.date_
join star."time" tt on tt.military_hour=t.hour_
where tt.time_key <> -1 
group by w.date_, tt.military_hour 
order by w.date_, tt.military_hour ;

truncate table star."bikeTrip";

insert into star."bikeTrip" (start_station_id, end_station_id, start_date, start_time, end_date, end_time, duration, air_temperatur_celsius, wind_speed_ms, precipitation_mm)
 select start_station_id, end_station_id, 
	start_date, start_time, 
	end_date, end_time, 
	duration, air_temperatur_celsius, 
	wind_speed_ms , precipitation_mm
	from bysykkel.temp_bike bt left join star.weather w 
	on bt.start_date = w.date  and bt.start_time = w.time_key;

create table star.stationTripsFull as
	with end_temp_t as (
		select bt.end_station_id station_id, bt.end_date date, t.military_hour, count(*) as trips_ended
		from star.station s 
			join star."bikeTrip" bt on bt.end_station_id = s.station_key_end
			join star."time" t on bt.end_time = t.time_key_end
		group by bt.end_station_id, bt.end_date, t.military_hour
	), start_temp_t as (
		select bt.start_station_id station_id, bt.start_date date, t.military_hour, count(*) as trips_started
		from star.station s 
			join star."bikeTrip" bt on bt.start_station_id = s.station_key_start
			join star."time" t on bt.start_time = t.time_key_start 
		group by bt.start_station_id, bt.start_date, t.military_hour
	), sst as (select coalesce(s.station_id, e.station_id) station_id, coalesce(s.date, e.date) date, coalesce(s.military_hour, e.military_hour) military_hour,
		coalesce(trips_ended, 0) trips_ended, coalesce(trips_started, 0) trips_started
	from start_temp_t s full outer join end_temp_t e on s.station_id = e.station_id and s.date = e.date and s.military_hour = e.military_hour
	) select station_id, s.date, s.military_hour, trips_ended, trips_started, air_temperatur_celsius, precipitation_mm, wind_speed_ms
	from sst s join star.weather_by_hour w on ((w.date=s.date)and(w.military_hour=s.military_hour));


create view star.present_stations as
select * from star.station where station_name not like '_LEGACY_';

alter table star.date
add column is_holiday int;

update star.date
set is_holiday = r.is_holiday
from bysykkel.reddays r 
where cast(r."Dates" as date) = date_actual;

alter table star.date
add column is_strike int;

update star.date
set is_strike = r.is_strike
from bysykkel.strikes r 
where cast(r.dob as date) = date_actual;

update star.date
set is_strike = COALESCE(is_strike, 0);

update star.date
set is_holiday = COALESCE(is_holiday, 0);

create view star.stationtripsview as
	with end_temp_t as (
		select bt.end_station_id station_id, bt.end_date date, t.military_hour, count(*) as trips_ended
		from star.station s 
			join star."bikeTrip" bt on bt.end_station_id = s.station_key_end
			join star."time" t on bt.end_time = t.time_key_end
		group by bt.end_station_id, bt.end_date, t.military_hour
	), start_temp_t as (
		select bt.start_station_id station_id, bt.start_date date, t.military_hour, count(*) as trips_started
		from star.station s 
			join star."bikeTrip" bt on bt.start_station_id = s.station_key_start
			join star."time" t on bt.start_time = t.time_key_start 
		group by bt.start_station_id, bt.start_date, t.military_hour
	), sst as (select coalesce(s.station_id, e.station_id) station_id, coalesce(s.date, e.date) date, coalesce(s.military_hour, e.military_hour) military_hour,
		coalesce(trips_ended, 0) trips_ended, coalesce(trips_started, 0) trips_started
	from start_temp_t s full outer join end_temp_t e on s.station_id = e.station_id and s.date = e.date and s.military_hour = e.military_hour
	order by coalesce(s.date, e.date), coalesce(s.military_hour, e.military_hour), coalesce(s.station_id, e.station_id)
	) select station_id, s.date, s.military_hour, trips_ended, trips_started, air_temperatur_celsius, precipitation_mm, wind_speed_ms
	from sst s join star.weather_by_hour w on ((w.date=s.date)and(w.military_hour=s.military_hour));


create view star.stationtrips_include_empty as
with station_hours as (select * from (select * from star.date where month_actual > 3) date
						cross join (select distinct(military_hour) from star.time where military_hour > 4) time
						cross join (select station_key from star.present_stations ps) station
						order by date_actual desc, military_hour
						) select date_key, s.military_hour, s.station_key, coalesce(trips_ended, 0) trips_ended, coalesce(trips_started, 0) trips_started,
						wh.air_temperatur_celsius, wh.precipitation_mm, wh.wind_speed_ms 
						from station_hours s
						left join (select * from star.stationtripsview where military_hour > 4) stat
						on stat.date = s.date_key and stat.military_hour = s.military_hour and stat.station_id=s.station_key
						join (select * from star.weather_by_hour where military_hour > 4) wh 
						on wh.military_hour = s.military_hour and s.date_key = wh.date;
					
