# Oslo CityBikes Analysis
Analysing Oslo City Bikes and finding out what affects users behaviour

Project for AW Academy

Malin, Camila, Carl

Problem:
--------------

The commune of Norway wants to better know what conditions most affect thebehaviour of the Oslo Bysykkel users.

For this project we want to answer the following questions:

-What are the most popular bicycle stands?

-What starting and ending points are most popular in each hour?

-What starting and ending points are most popular each day of the week for eachmonth?

-How does weather change the behaviour of oslo bysykkel users.

-How does red days affect the behaviour of bysykkel users

-How does public transport strikes affect bysykkel users behaviour.

-Given a starting point and time, what is the most likely ending point for that trip?

Project scope:
------------------

-Create a Postgres Database running on a Cloud Platform.

-Use data from Oslo bysykkel (csv format), FROSTAPI (JSON), GOOGLE API(JSON) and information about red days, strikes and week days (self made) to enrich Database.

-Set up star schema Data Mart for accessing data about each bicycle trip.

-Set up workload to update the Database daily using Airflow.

-Use Power BI to visualize the data in order to answer the questions above.

-Create a ML model that predicts the daily use of bicicles in each stand.

