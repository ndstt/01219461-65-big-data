-- Lab Hive vs Pig - Q2.1 (Pig)
-- Top 5 maximum procedure time period with patients and encounters

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

procedures_dt = FOREACH procedures GENERATE
  patient,
  encounter,
  REPLACE(REPLACE(start_time, 'T', ' '), 'Z', '') AS start_clean,
  REPLACE(REPLACE(stop_time, 'T', ' '), 'Z', '') AS stop_clean;

procedures_dur = FOREACH procedures_dt GENERATE
  patient,
  encounter,
  start_clean,
  stop_clean,
  (ToUnixTime(ToDate(stop_clean, 'yyyy-MM-dd HH:mm:ss')) -
   ToUnixTime(ToDate(start_clean, 'yyyy-MM-dd HH:mm:ss'))) AS duration_sec;

procedures_dur_ok = FILTER procedures_dur BY duration_sec IS NOT NULL;
proc_pat = JOIN procedures_dur_ok BY patient LEFT, patients BY id;
proc_pat_flat = FOREACH proc_pat GENERATE
  procedures_dur_ok::patient AS patient_id,
  patients::first AS first,
  patients::last AS last,
  procedures_dur_ok::encounter AS encounter_id,
  procedures_dur_ok::start_clean AS start_time,
  procedures_dur_ok::stop_clean AS stop_time,
  procedures_dur_ok::duration_sec AS duration_sec;

proc_pat_sorted = ORDER proc_pat_flat BY duration_sec DESC;
q21_top5 = LIMIT proc_pat_sorted 5;
DUMP q21_top5;
