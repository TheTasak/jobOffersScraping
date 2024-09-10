#!/bin/bash

# copy polish dictionary files to postgres dir
sudo cp polish.affix "$(pg_config --sharedir)/tsearch_data/"
sudo cp polish.dict "$(pg_config --sharedir)/tsearch_data/"
sudo cp polish.stop "$(pg_config --sharedir)/tsearch_data/"

# create db
psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'jobs'" | grep -q 1 || psql -U postgres -c "CREATE DATABASE jobs"

#create tables
psql -U postgres -d jobs -c "CREATE TABLE IF NOT EXISTS jobs (
	id INTEGER NOT NULL,
	source VARCHAR(20),
	url VARCHAR(250),
	job_title VARCHAR(200),
	location VARCHAR(150),
	category VARCHAR(100),
	company VARCHAR(100),
	salary VARCHAR(200),
	type_of_work VARCHAR(70),
	experience VARCHAR(100),
	employment_type VARCHAR(100),
	operating_mode VARCHAR(120),
	skills TEXT,
	description TEXT,
	PRIMARY KEY(id, source)
);"
psql -U postgres -d jobs -c "CREATE TABLE IF NOT EXISTS occurrences (
	occurence_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	id INTEGER NOT NULL,
	source VARCHAR(20),
	created_at TIMESTAMP,
	FOREIGN KEY(id, source) REFERENCES jobs(id, source)
);"

# create dictionary
psql -U postgres -d jobs -c "CREATE TEXT SEARCH DICTIONARY pl_ispell (
  Template = ispell,
  DictFile = polish,
  AffFile = polish,
  StopWords = polish
);"
psql -U postgres -d jobs -c "CREATE TEXT SEARCH CONFIGURATION pl_ispell(parser = default);"
psql -U postgres -d jobs -c "ALTER TEXT SEARCH CONFIGURATION pl_ispell
ALTER MAPPING FOR asciiword, asciihword, hword_asciipart, word, hword, hword_part WITH pl_ispell;"

psql -U postgres -d jobs -c "ALTER TABLE jobs ADD COLUMN description_search_en tsvector GENERATED ALWAYS AS (to_tsvector('english', description)) STORED;"
psql -U postgres -d jobs -c "ALTER TABLE jobs ADD COLUMN description_search_pl tsvector GENERATED ALWAYS AS (to_tsvector('pl_ispell', description)) STORED;"