 -- Load Data in Pig from Hdfs  
 
 Pigdata = LOAD 'hdfs://localhost:9000/pig_project_data/pigdata.csv' USING PigStorage(',') 
 AS (YEAR :int, QUARTER: int, MONTH :int, DAY_OF_MONTH :int, DAY_OF_WEEK :int, 
 FL_DATE :chararray,OP_UNIQUE_CARRIER :chararray, ORIGIN :chararray, ORIGIN_CITY_NAME: chararray,
  DEST :chararray, DEST_CITY_NAME :chararray, CRS_DEP_TIME: int, DEP_DELAY: int,CRS_ARR_TIME :int, 
 ARR_TIME: int, ARR_DELAY :int, CANCELLED :int, DIVERTED: int, DISTANCE: int);




-- Top 5 out traffic for origins
OUTFLIGHT = FOREACH Pigdata GENERATE MONTH AS m, ORIGIN AS d;

GROUP_OUTFLIGHT = GROUP OUTFLIGHT BY (m,d);

COUNT_OUTFLIGHT = FOREACH GROUP_OUTFLIGHT GENERATE FLATTEN(group), COUNT(OUTFLIGHT) AS count;

GROUP_COUNT_OUTFLIGHT = GROUP COUNT_OUTFLIGHT BY m;

topMonthlyOUTFLIGHT = FOREACH GROUP_COUNT_OUTFLIGHT {
    result = TOP(5, 2, COUNT_OUTFLIGHT); 
    GENERATE FLATTEN(result);
}

-- Show output
dump topMonthlyOUTFLIGHT

STORE topMonthlyOUTFLIGHT INTO 'hdfs://localhost:9000/PigOutput_Dharti/Top_5_Out_Flight' USING PigStorage(',');
