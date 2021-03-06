# Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Database Schema
In this project using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.


### songplays (Fact Table)

| Column      | Data type | Constraint                     |
|-------------|-----------|--------------------------------|
| songplay_id | SERIAL    | PRIMARY KEY                    |
| start_time  | TIMESTAMP | REFERENCES time (start_time)   |
| user_id     | INT       | REFERENCES users (user_id)     |
| level       | VARCHAR   |                                |
| song_id     | VARCHAR   | REFERENCES songs (song_id)     |
| artist_id   | VARCHAR   | REFERENCES artists (artist_id) |
| session_id  | INT       | NOT NULL                       |
| location    | VARCHAR   |                                |
| user_agent  | VARCHAR   |                                |

### users (Dimension Table)

| Column     | Data type  | Constraint  |
|------------|------------|-------------|
| user_id    | INT        | PRIMARY KEY |
| first_name | VARCHAR    | NOT NULL    |
| last_name  | VARCHAR    | NOT NULL    |
| gender     | VARCHAR(1) |             |
| level      | VARCHAR    |             |

### songs (Dimension Table)

| Column    | Data type | Constraint        |
|-----------|-----------|-------------------|
| song_id   | VARCHAR   | PRIMARY KEY       |
| title     | VARCHAR   | NOT NULL          |
| artist_id | VARCHAR   | NOT NULL          |
| year      | INT       | CHECK (year >= 0) |
| duration  | FLOAT     |                   |

### artists (Dimension Table)
- artists in music database

| Column    | Data type | Constraint  |
|-----------|-----------|-------------|
| artist_id | VARCHAR   | PRIMARY KEY |
| name      | VARCHAR   | NOT NULL    |
| location  | VARCHAR   |             |
| latitude  | FLOAT     |             |
| longitude | FLOAT     |             |

### time (Dimension Table)

| Column     | Data Type | Constraint                                  |
|------------|-----------|---------------------------------------------|
| start_time | TIMESTAMP | PRIMARY KEY                                 |
| hour       | INT       | NOT NULL CHECK (hour >= 0 and hour <= 24)   |
| day        | INT       | NOT NULL CHECK (day >= 0 and day <= 366)    |
| week       | INT       | NOT NULL CHECK (week >= 0 and week <= 55)   |
| month      | INT       | NOT NULL CHECK (month >= 0 and month <= 12) |
| year       | INT       | NOT NULL CHECK (year >= 0)                  |
| weekday    | INT       | (weekday >= 0 and weekday <= 7)             |

## ETL Pipeline

### Directory Structure
```
├── README.md
├── create_tables.py
├── data
├── etl.ipynb
├── etl.py
├── sql_queries.py
└── test.ipynb


NOTE: You will not be able to run test.ipynb, etl.ipynb, or etl.py until you have run create_tables.py at least once to create the sparkifydb database, which these other files connect to.