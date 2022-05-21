# Data Warehouse with AWS Redshift

## Project description

The purpose of this project is to build a data warehouse onto the cloud for a music streaming startup, Sparkify, that will use it for analysing their data.

In this project we built an ETL pipeline for a database hosted on Redshift: we extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics team.

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app and the artist of those songs.

## The purpose of the data warehouse

It contains four dimension tables for users, songs, artist, time and a fact table for songplays.
It finding insights into what songs their users are listening to and permits for a wide range of queries and analysis: about users, songs and artists, we can query about the most listened songs, the most poplar artists, what every users listens the most whitch can help for recommending similar content and inderstanding the trends of listenings.

## How to run the Python scripts:

First of all we need to create a config file `dwh.cfg` to put _**Cluster**_ parameters, _**IAM role**_ link and _**S3**_ links. The file `dwh.cfg.example` can be renamed and filled with proper config parameters.
We then need to run the `create_tables.py` script. It dorps the tables if they exist and build fresh ones (the queries for tables dropping and creation are implemented in the `sql_queries.py`). 
Then we just need to run the `etl.py` script that first loads the json files (`log_data` and `songs_data`) data from S3 into the two staging tables: `events_staging` and `songs_staging`. Then populates the data base (dimensions and fact table).

Inside a terminal run these commands:

```sh
python create_tables.py
python etl.py
```

## Schema:

The schema of our database is a star schema. It consist of a fact table `songplays` that references four dimension tables: `artists`, `users`, `song`, `time`. It allows to simplifie querries and makes aggregations fast.

## ETL:

The process is rather simple, we extract the data from the json files and put them into pandas dataframe where we can keep the columns that we want and drop the rest. We also perform timestamps transformation to datetime. The only non straight forward transformation is the one for the songplays table, where we needed to querie the dataset about song_id and artist_id (instead of keeping names) ids are better foreign keys.

## Example queries

Once our data warehouse is populated with data, we can go and query it to get analitical insights.
For example, the query below returns the Top 3 streamed songs during november 2018:
**Query**
```sql
SELECT 
    s.title, COUNT(*) AS streaming_count
FROM 
    songplays p 
    LEFT JOIN 
        time t 
    ON p.start_time = t.start_time
    LEFT JOIN 
        songs s 
    ON s.song_id = p.song_id
WHERE 
    s.title IS NOT NULL AND
    t.year = 2018 AND 
    t.month = 11  
GROUP BY 
    s.title
ORDER BY 
    streaming_count DESC
LIMIT 3;
```

**Result**

| title                                                | streaming_count |
| ---------------------------------------------------- | --------------- |
| You're The One                                       | 37              |
| I CAN'T GET STARTED                                  | 9               |
| Catch You Baby (Steve Pitron & Max Sanna Radio Edit) | 9               |