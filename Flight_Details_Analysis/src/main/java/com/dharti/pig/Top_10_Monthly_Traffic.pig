 -- Load Data in Pig from Hdfs  
 Pigdata = LOAD 'hdfs://localhost:9000/pig_project_data/pigdata.csv' USING PigStorage(',') 
 AS (YEAR :int, QUARTER: int, MONTH :int, DAY_OF_MONTH :int, DAY_OF_WEEK :int, 
 FL_DATE :chararray,OP_UNIQUE_CARRIER :chararray, ORIGIN :chararray, ORIGIN_CITY_NAME: chararray,
  DEST :chararray, DEST_CITY_NAME :chararray, CRS_DEP_TIME: int, DEP_DELAY: int,CRS_ARR_TIME :int, 
 ARR_TIME: int, ARR_DELAY :int, CANCELLED :int, DIVERTED: int, DISTANCE: int);



-- Find Total traffic per month
UNION_TRAFFIC = UNION COUNT_INFLIGHT, COUNT_OUTFLIGHT;

GROUP_UNION_TRAFFIC = GROUP UNION_TRAFFIC BY (m,d);

TOTAL_TRAFFIC = FOREACH GROUP_UNION_TRAFFIC GENERATE FLATTEN(group) AS (m,code), SUM(UNION_TRAFFIC.count) AS total; 

TOTAL_MONTHLY = GROUP TOTAL_TRAFFIC BY m;

topMonthlyTraffic = FOREACH TOTAL_MONTHLY {
    result = TOP(10, 2, TOTAL_TRAFFIC); 
    GENERATE FLATTEN(result) AS (month, iata, traffic);
}


STORE topMonthlyTraffic INTO 'hdfs://localhost:9000/PigOutput_Dharti/Total_Monthly_Traffic' USING PigStorage(',');

explain -brief -dot -out ./ topMonthlyTraffic
