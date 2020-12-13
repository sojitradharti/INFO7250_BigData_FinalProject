 -- Load Data in Pig from Hdfs  
 Pigdata = LOAD 'hdfs://localhost:9000/pig_project_data/pigdata.csv' USING PigStorage(',') 
 AS (YEAR :int, QUARTER: int, MONTH :int, DAY_OF_MONTH :int, DAY_OF_WEEK :int, 
 FL_DATE :chararray,OP_UNIQUE_CARRIER :chararray, ORIGIN :chararray, ORIGIN_CITY_NAME: chararray,
  DEST :chararray, DEST_CITY_NAME :chararray, CRS_DEP_TIME: int, DEP_DELAY: int,CRS_ARR_TIME :int, 
 ARR_TIME: int, ARR_DELAY :int, CANCELLED :int, DIVERTED: int, DISTANCE: int);


Top_Carrier = FOREACH Pigdata GENERATE ORIGIN AS o, DEST AS d;


Group_Carrier = GROUP Top_Carrier by (s,d);

Total = FOREACH Group_Carrier GENERATE group, COUNT(Top_Carrier) AS CNT;

ASCORDER = ORDER Total BY CNT DESC;

--dump Total;
STORE INVORDER INTO 'hdfs://localhost:9000/PigOutput_Dharti/Most_Popular_Origin_Destinations' USING PigStorage(',');