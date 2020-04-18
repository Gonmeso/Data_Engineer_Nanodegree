#!/bin/bash
echo "################### Creating Data Warehouse ###################"
python deploy.py
echo "################### Creating Star Schema ###################"
python create_tables.py
echo "################### Performing ETL on S3 Data ###################"
python etl.py
echo "################### Process Finished ###################"