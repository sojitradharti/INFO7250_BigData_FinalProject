 -- Load Data in Pig from Hdfs   
 Pigdata = LOAD 'hdfs://localhost:9000/pig_project_data/pigdata.csv' USING PigStorage(',') 
 AS (YEAR :int, QUARTER: int, MONTH :int, DAY_OF_MONTH :int, DAY_OF_WEEK :int, 
 FL_DATE :chararray,OP_UNIQUE_CARRIER :chararray, ORIGIN :chararray, ORIGIN_CITY_NAME: chararray,
  DEST :chararray, DEST_CITY_NAME :chararray, CRS_DEP_TIME: int, DEP_DELAY: int,CRS_ARR_TIME :int, 
 ARR_TIME: int, ARR_DELAY :int, CANCELLED :int, DIVERTED: int, DISTANCE: int);


-- Top 5 In flights for unique destinations
INFLIGHT = FOREACH Pigdata GENERATE MONTH AS m, DEST AS d;

-- group by month December, sort ID
GROUP_INFLIGHT = GROUP INFLIGHT BY (m,d);

-- aggregate and flatten group so that output relation has 3 fields
COUNT_INFLIGHT = FOREACH GROUP_INFLIGHT GENERATE FLATTEN(group), COUNT(INFLIGHT) AS count;

-- aggregate over months
GROUP_COUNT_INFLIGHT = GROUP COUNT_INFLIGHT BY m;

-- apply UDF to compute top k (k=10)
topInFlight= FOREACH GROUP_COUNT_INFLIGHT{result = TOP(5, 2, COUNT_INFLIGHT); GENERATE FLATTEN(result);}


-- dump topInFlight
STORE topInFlight INTO 'hdfs://localhost:9000/PigOutput_Dharti/Top_5_In_Flight' USING PigStorage(',');



