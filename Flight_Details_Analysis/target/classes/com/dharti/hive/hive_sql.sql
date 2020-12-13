-- Create schema
create schema FlightSchema;

-- change current schema
use FlightSchema;


-- create table with data types
create external table flights(YEAR INT, QUARTER INT, MONTH INT, DAY_OF_MONTH INT, DAY_OF_WEEK INT, FL_DATE DATE,OP_UNIQUE_CARRIER String, ORIGIN String, ORIGIN_CITY_NAME String, DEST String, DEST_CITY_NAME String, CRS_DEP_TIME INT, DEP_DELAY INT,CRS_ARR_TIME INT, ARR_TIME INT, ARR_DELAY Float, CANCELLED INT, DIVERTED INT, DISTANCE INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- load data in new table from hdfs
--NEWDATA1 IS WORKING FOR DATE

--hadoop fs -copyFromLocal   /Users/dharatibensojitra/Documents/BigData/FinalProject/newdata.csv /projectdata/flight

-- load data in new table from hdfs
LOAD DATA INPATH 'hdfs://localhost:9000/projectdata/flight' OVERWRITE INTO TABLE flights;

-- first import data in hadoop
-- hadoop fs -copyFromLocal   /Users/dharatibensojitra/Documents/BigData/FinalProject/airports.csv /projectdata/airports.csv

create table airports (Iata String, airport String, city String, state String, country String, lat String, long String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


LOAD DATA INPATH 'hdfs://localhost:9000/projectdata/airports' OVERWRITE INTO TABLE airports;

---
create external table carriers (Code String, Description String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;


--hadoop fs -copyFromLocal   /Users/dharatibensojitra/Documents/BigData/FinalProject/carriers.csv /projectdata/carriers

-- Loading data to table flightschema.carriers
LOAD DATA INPATH 'hdfs://localhost:9000/projectdata/carriers' OVERWRITE INTO TABLE carriers;

---
INSERT OVERWRITE DIRECTORY '/HiveOutput_Dharti/Count_FlightTime_1000' select count(*) as countFlights from flights as f where f.ARR_TIME > 2000;

---
INSERT OVERWRITE DIRECTORY '/HiveOutput_Dharti/ArrTime' select f.FL_DATE,f.ORIGIN,f.DEST from flights as f where f.ARR_TIME <f.CRS_ARR_TIME;

---
INSERT OVERWRITE DIRECTORY '/HiveOutput_Dharti/Total_Cancelled_flights_Per_Unique_Code' Select carriers.description, uniqueCount.countCancelled, uniqueCount.TotalCarrier from (Select OP_UNIQUE_CARRIER, count(*) as TotalCarrier, sum(cancelled) as countCancelled from flights group by OP_UNIQUE_CARRIER) AS uniqueCount, carriers where carriers.code = uniqueCount.OP_UNIQUE_CARRIER;

---
INSERT OVERWRITE DIRECTORY '/HiveOutput_Dharti/TotalFlights_From_Boston_1988' SELECT COUNT(*) FROM FLIGHTS f WHERE f.Origin = 'BOS' AND f.Year = 1988;

INSERT OVERWRITE DIRECTORY '/HiveOutput_Dharti/TotalFlights_From_Boston_Each_Destination' 
SELECT f.ORIGIN, f.DEST,COUNT(*) FROM FLIGHTS f
WHERE f.Origin = 'BOS' AND f.Year = 1988
GROUP BY f.Origin, f.Dest;





