# Project: Data Warehouse

### Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


### Project Description

In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

Here are the steps you will take to create the schema for your tables and then copy teh data from Amazon S3 to Amazon Redshift. 

1. Design schemas for your fact and dimension tables. We will create multiple dimension tables and single fact table. The schema for the tables is given below:

    a. Fact Table: songplays - records in event data associated with song plays i.e. records with page NextSong with variables `songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`
    b. Dimension Tables: There are 4 dimension tables `users` table, `songs` table, `artists` table and `time` table.
        1. users - users in the app. Variables: `user_id, first_name, last_name, gender, level`
        2. songs - songs in music database. Variables: `song_id, title, artist_id, year, duration`
        3. artists - artists in music database. Variables: `artist_id, name, location, lattitude, longitude`
        4. time - timestamps of records in songplays broken down into specific units. Variables: `start_time, hour, day, week, month, year, weekday`
        


