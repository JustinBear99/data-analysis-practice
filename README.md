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
Save the processed data back to the database with new tables.

## Visualization
Open `visualization.ipynb` for data visualization and detailed exlpanation.