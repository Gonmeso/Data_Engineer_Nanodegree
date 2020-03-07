#!/bin/bash
echo "Waiting for DB"
sleep 10
echo "Launching database and tables creation\n"
python create_tables.py
echo "Performing data ETL\n"
python etl.py
echo "Fin"