#!/bin/bash
echo "################### Creating Data Warehouse ###################"
python deploy.py
echo "################### Creating Star Schema ###################"
python create_tables.py
echo "################### Process Finished ###################"