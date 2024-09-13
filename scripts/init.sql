CREATE TABLE IF NOT EXISTS jobs (
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
);

CREATE TABLE IF NOT EXISTS occurrences (
	occurrence_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	id INTEGER NOT NULL,
	source VARCHAR(20),
	created_at TIMESTAMP,
	FOREIGN KEY(id, source) REFERENCES jobs(id, source)
);

CREATE TEXT SEARCH DICTIONARY pl_ispell (
  Template = ispell,
  DictFile = polish,
  AffFile = polish,
  StopWords = polish
);

CREATE TEXT SEARCH CONFIGURATION pl_ispell(parser = default);

ALTER TEXT SEARCH CONFIGURATION pl_ispell
ALTER MAPPING FOR asciiword, asciihword, hword_asciipart, word, hword, hword_part WITH pl_ispell;

ALTER TABLE jobs ADD COLUMN description_search_en tsvector GENERATED ALWAYS AS (to_tsvector('english', description)) STORED;
ALTER TABLE jobs ADD COLUMN description_search_pl tsvector GENERATED ALWAYS AS (to_tsvector('pl_ispell', description)) STORED;