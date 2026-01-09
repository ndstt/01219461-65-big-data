-- Lab Hive vs Pig - Q2.2 (Pig)
-- Top 5 maximum procedure costs with payers and patients

procedures_raw = LOAD '/homework/hive-vs-pig/procedures/procedures.csv' USING PigStorage(',') AS (
  start_time:chararray,
  stop_time:chararray,
  patient:chararray,
  encounter:chararray,
  code:chararray,
  description:chararray,
  base_cost:double,
  reasoncode:chararray,
  reasondescription:chararray
);
procedures = FILTER procedures_raw BY start_time MATCHES '^[0-9]{4}-.*';

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

patients_raw = LOAD '/homework/hive-vs-pig/patients/patients.csv' USING PigStorage(',') AS (
  id:chararray,
  birthdate:chararray,
  deathdate:chararray,
  prefix:chararray,
  first:chararray,
  last:chararray,
  suffix:chararray,
  maiden:chararray,
  marital:chararray,
  race:chararray,
  ethnicity:chararray,
  gender:chararray,
  birthplace:chararray,
  address:chararray,
  city:chararray,
  state:chararray,
  county:chararray,
  zip:chararray,
  lat:double,
  lon:double
);
patients = FILTER patients_raw BY id MATCHES '^[0-9a-f].*';

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

proc_enc = JOIN procedures BY encounter, encounters BY id;
proc_enc_flat = FOREACH proc_enc GENERATE
  procedures::patient AS patient_id,
  procedures::encounter AS encounter_id,
  procedures::base_cost AS procedure_cost,
  encounters::payer AS payer_id;

proc_payer = JOIN proc_enc_flat BY payer_id LEFT, payers BY id;
proc_payer_pat = JOIN proc_payer BY proc_enc_flat::patient_id LEFT, patients BY id;
proc_cost_flat = FOREACH proc_payer_pat GENERATE
  proc_enc_flat::procedure_cost AS procedure_cost,
  proc_enc_flat::patient_id AS patient_id,
  patients::first AS first,
  patients::last AS last,
  payers::name AS payer_name,
  proc_enc_flat::encounter_id AS encounter_id;

proc_cost_sorted = ORDER proc_cost_flat BY procedure_cost DESC;
q22_top5 = LIMIT proc_cost_sorted 5;
DUMP q22_top5;
