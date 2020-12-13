 -- Load Data in Pig from Hdfs  
 Pigdata = LOAD 'hdfs://localhost:9000/pig_project_data/pigdata.csv' USING PigStorage(',') 
 AS (YEAR :int, QUARTER: int, MONTH :int, DAY_OF_MONTH :int, DAY_OF_WEEK :int, 
 FL_DATE :chararray,OP_UNIQUE_CARRIER :chararray, ORIGIN :chararray, ORIGIN_CITY_NAME: chararray,
  DEST :chararray, DEST_CITY_NAME :chararray, CRS_DEP_TIME: int, DEP_DELAY: int,CRS_ARR_TIME :int, 
 ARR_TIME: int, ARR_DELAY :int, CANCELLED :int, DIVERTED: int, DISTANCE: int);



CARRIER_DATA = FOREACH Pigdata GENERATE month AS m, carrier AS cname;
GROUP_CARRIERS = GROUP CARRIER_DATA BY (m,cname);
COUNT_CARRIERS = FOREACH GROUP_CARRIERS GENERATE FLATTEN(group), (COUNT(CARRIER_DATA)) AS popularity;

STORE COUNT_CARRIERS INTO 'hdfs://localhost:9000/PigOutput_Dharti/Most_Used_Carriers)' USING PigStorage(',');