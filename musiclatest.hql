----Loading data files into HIVE TABLES FROM HDFS
----CREATION OF HIVE DATABASE IF DOES NOT EXIST
CREATE DATABASE IF NOT EXISTS musicdba;
----SETTING THE HIVE DATABASE
USE musicdba;
----CREATION OF HIVE TABLE WHICH CAN HANDLE DATA IN TEXTFILE FORMAT
CREATE TABLE IF NOT EXISTS datasource1
(user_id STRING
,song_id STRING
,artist_id STRING
,timestamp STRING
,start_ts STRING
,end_ts STRING
,geo_cd STRING
,station_id STRING
,song_end_type INT
,like INT
,dislike INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE
;
----LOADING DATA FROM TEXT FILE INTO HIVE TABLE
LOAD DATA INPATH '/user/cloudera/chhaya/hivedata/file.txt'
OVERWRITE INTO TABLE datasource1;
----ADDING JAR TO HANDLE XML DATA AND CREATE A TABLE SPECIFICALLY FOR XML DATA
ADD jar /home/cloudera/Downloads/hivexmlserde-1.0.5.3.jar;
USE musicdba ;
----CREATION OF TABLE WHICH CAN HANDLE XML DATA
CREATE TABLE IF NOT EXISTS datasource2(
user_id STRING
,song_id STRING
,artist_id STRING
,timestamp STRING
,start_ts STRING
,end_ts STRING
,geo_cd STRING
,station_id STRING
,song_end_type INT
,like INT
,dislike INT
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.user_id"="/record/user_id/text()",
"column.xpath.song_id"="/record/song_id/text()",
"column.xpath.artist_id"="/record/artist_id/text()",
"column.xpath.timestamp"="/record/timestamp/text()",
"column.xpath.start_ts"="/record/start_ts/text()",
"column.xpath.end_ts"="/record/end_ts/text()",
"column.xpath.geo_cd"="/record/geo_cd/text()",
"column.xpath.station_id"="/record/station_id/text()",
"column.xpath.song_end_type"="/record/song_end_type/text()",
"column.xpath.like"="/record/like/text()",
"column.xpath.dislike"="/record/dislike/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
TBLPROPERTIES (
"xmlinput.start"="<record>",
"xmlinput.end"="</record>"
);
----LOADING XML DATA TO HIVE TABLE DEDICATED FOR XML FILE

LOAD DATA INPATH '/user/cloudera/chhaya/hivedata/file.xml' 
OVERWRITE INTO TABLE datasource2;

----INSERTING DATA FROM TABLE1 OF FILE.XML TO TABLE2 OF FILE.XML

INSERT INTO TABLE datasource1 
SELECT *
FROM datasource2; 

----LEAVING THE HIVE PROMPT
quit;
