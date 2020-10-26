# Oslo CityBikes Analysis
Builds a postgres Database with all data about Oslo City bikes since 2016.
Uses PowerBI to visualize data and get insights.

To create the database run the following comand in the main folder.
docker-compose up --build -d

to run airflow run docker-compose on the airflow folder.
The following image is the visualization of the program workflow.
![workflow](/visualization/initdbs.png)


##Problem scope:
The commune of Norway wants to better know what conditions most affect thebehaviour of the Oslo Bysykkel users.

For this project we want to answer the following questions:
-What are the most popular bicycle stands?
-What starting and ending points are most popular in each hour?
-What starting and ending points are most popular each day of the week for eachmonth?
-How does weather change the behaviour of oslo bysykkel users.
-How does red days affect the behaviour of bysykkel users
-How does public transport strikes affect bysykkel users behaviour.
