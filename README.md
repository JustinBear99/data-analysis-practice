# Data analysis for all activities on [opendata](https://data.gov.tw/)
The ETL process record of analyzing all held activities.

## Extract
```
python3 fetch_raw.py
```
Connect to AWS RDS with config.json, create database, create tables, and insert all raw data from opendata.
Tables include `activity`, `showInfo`, `masterUnit`, etc.

## Transform
```
python3 pre_process.py
```
Perform three default analysis, and return `pandas.DataFrame` or `dict`.

## Load
```
python3 insert_to_db.py
```
Save the processed data back to the database with new tables `topActivity` and `cityCategories`.

## Visualization
Open `visualization.ipynb` for data visualization and detailed exlpanation.

## About config.json
The information stored in config.json is like below:
```
{
    "user": "username",
    "password": "userpassword",
    "host": "opendata.c2kvf8hxkoaf.us-east-1.rds.amazonaws.com"
}
```
If you want to access the database, please contact me. Thanks!