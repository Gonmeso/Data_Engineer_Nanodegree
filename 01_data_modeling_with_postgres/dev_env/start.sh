#!/bin/bash
echo "Waiting for DB"
sleep 20
echo "Launching database and tables creation\n"
python create_tables.py
echo "Performing data ETL\n"
python etl.py
echo "Fin"