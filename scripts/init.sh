#!/bin/bash

# copy polish dictionary files to postgres dir
sudo cp polish.affix "$(pg_config --sharedir)/tsearch_data/"
sudo cp polish.dict "$(pg_config --sharedir)/tsearch_data/"
sudo cp polish.stop "$(pg_config --sharedir)/tsearch_data/"

# create db
psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'jobs'" | grep -q 1 || psql -U postgres -c "CREATE DATABASE jobs"
psql -U postgres -d jobs -a -f "init.sql"