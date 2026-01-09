-- Hive setup derived from create_hospital_db.sql (MySQL)
CREATE DATABASE IF NOT EXISTS hospital_db;
USE hospital_db;

CREATE EXTERNAL TABLE IF NOT EXISTS payers (
  id string,
  name string,
  address string,
  city string,
  state_headquartered string,
  zip string,
  phone string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/homework/hive-vs-pig/payers'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS patients (
  id string,
  birthdate string,
  deathdate string,
  prefix string,
  first string,
  last string,
  suffix string,
  maiden string,
  marital string,
  race string,
  ethnicity string,
  gender string,
  birthplace string,
  address string,
  city string,
  state string,
  county string,
  zip string,
  lat double,
  lon double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/homework/hive-vs-pig/patients'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS organizations (
  id string,
  name string,
  address string,
  city string,
  state string,
  zip string,
  lat double,
  lon double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/homework/hive-vs-pig/organizations'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS encounters (
  id string,
  start_time string,
  stop_time string,
  patient string,
  organization string,
  payer string,
  encounterclass string,
  code string,
  description string,
  base_encounter_cost double,
  total_claim_cost double,
  payer_coverage double,
  reasoncode string,
  reasondescription string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/homework/hive-vs-pig/encounters'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS procedures (
  start_time string,
  stop_time string,
  patient string,
  encounter string,
  code string,
  description string,
  base_cost double,
  reasoncode string,
  reasondescription string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/homework/hive-vs-pig/procedures'
TBLPROPERTIES ('skip.header.line.count'='1');
