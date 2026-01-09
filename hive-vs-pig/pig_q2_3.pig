-- Lab Hive vs Pig - Q2.3 (Pig)
-- Payer organization, payer coverage, encounter, total claim cost for year 2022

encounters_raw = LOAD '/homework/hive-vs-pig/encounters/encounters.csv' USING PigStorage(',') AS (
  id:chararray,
  start_time:chararray,
  stop_time:chararray,
  patient:chararray,
  organization:chararray,
  payer:chararray,
  encounterclass:chararray,
  code:chararray,
  description:chararray,
  base_encounter_cost:double,
  total_claim_cost:double,
  payer_coverage:double,
  reasoncode:chararray,
  reasondescription:chararray
);
encounters = FILTER encounters_raw BY id MATCHES '^[0-9a-f].*';

payers_raw = LOAD '/homework/hive-vs-pig/payers/payers.csv' USING PigStorage(',') AS (
  id:chararray,
  name:chararray,
  address:chararray,
  city:chararray,
  state_headquartered:chararray,
  zip:chararray,
  phone:chararray
);
payers = FILTER payers_raw BY id MATCHES '^[0-9a-f].*';

enc_2022 = FILTER encounters BY SUBSTRING(start_time, 0, 4) == '2022';
enc_pay = JOIN enc_2022 BY payer LEFT, payers BY id;
q23 = FOREACH enc_pay GENERATE
  payers::name AS payer_organization,
  enc_2022::payer_coverage AS payer_coverage,
  enc_2022::id AS encounter_id,
  enc_2022::total_claim_cost AS total_claim_cost;

q23_sorted = ORDER q23 BY total_claim_cost DESC;
DUMP q23_sorted;
