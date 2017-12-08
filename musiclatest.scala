//READING HBASE LOOKUP TABLES IN SPARK
//Importing the SPARK PACKAGES AND HBASE PACKAGES
import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import sqlContext.implicits._
import org.apache.hadoop.hbase.client._
//====================================================================
//LOADING HBASE LOOKUP TABLE Station_Geo_Map into SPARK DATAFRAME

val tableName1="Station_Geo_Map"
val hconf1 = HBaseConfiguration.create()
hconf1.set(TableInputFormat.INPUT_TABLE, tableName1)
val admin = new HBaseAdmin(hconf1)
val hBaseRDD1 = sc.newAPIHadoopRDD(hconf1, 
classOf[TableInputFormat],
classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], 
classOf[org.apache.hadoop.hbase.client.Result])
val result1 = hBaseRDD1.count()
val resultRDD1 = hBaseRDD1.map(tuple => tuple._2) 
val keyValueRDD1 = resultRDD1.map(result1 =>
                 (Bytes.toString(result1.getRow()).split(" ")(0),
                  Bytes.toString(result1.value)))
keyValueRDD1.collect()
//DEFINING THE CASE CLASS FOR HBASE TABLE
case class stationgeomap(stationid:String,geocd:String)
//CONVERSION FROM CASE CLASS TO SPARK DATAFRAME
val stationgeodf = keyValueRDD1.map{ case (s0,s1)=> stationgeomap(s0,s1)}.toDF()
stationgeodf.registerTempTable("Station_Geo_Map")
sqlContext.sql("select * from Station_Geo_Map").show()
stationgeodf.show()
//====================================================================
//LOADING HBASE LOOKUP TABLE Subscribed_Users into SPARK DATAFRAME

val tableName2="Subscribed_Users"
val hconf2 = HBaseConfiguration.create()
hconf2.set(TableInputFormat.INPUT_TABLE, tableName2)
val admin = new HBaseAdmin(hconf2)
val hBaseRDD2 = sc.newAPIHadoopRDD(hconf2, 
classOf[TableInputFormat],
classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], 
classOf[org.apache.hadoop.hbase.client.Result])
val result2 = hBaseRDD2.count()
val resultRDD2 = hBaseRDD2.map(tuple => tuple._2) 
val keyValueRDD2 = resultRDD2.map(result2 =>
                 (Bytes.toString(result2.getRow()).split(" ")(0),
                  Bytes.toString(result2.value),
                  Bytes.toString(result2.value)
                 ))
keyValueRDD2.collect()
//DEFINING THE CASE CLASS FOR HBASE TABLE
case class Subscribed_Users(userid:String,subscription_start_date:String,subscription_end_date:String)
//CONVERSION FROM CASE CLASS TO SPARK DATAFRAME
val subscribedusersdf = keyValueRDD2.map{ case (s0,s1,s2)=> Subscribed_Users(s0,s1,s2)}.toDF()
subscribedusersdf.registerTempTable("Subscribed_Users")
sqlContext.sql("select * from Subscribed_Users").show()
subscribedusersdf.show()
//==================================================================
//LOADING HBASE LOOKUP TABLE Song_Artist_Map INTO SPARK DATAFRAME

val tableName3 ="Song_Artist_Map"
val hconf3 = HBaseConfiguration.create()
hconf3.set(TableInputFormat.INPUT_TABLE, tableName3)
val admin = new HBaseAdmin(hconf3)
val hBaseRDD3 = sc.newAPIHadoopRDD(hconf3, 
classOf[TableInputFormat],
classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], 
classOf[org.apache.hadoop.hbase.client.Result])
val result3 = hBaseRDD3.count()
val resultRDD3 = hBaseRDD3.map(tuple => tuple._2) 
val keyValueRDD3 = resultRDD3.map(result3 =>
                 (Bytes.toString(result3.getRow()).split(" ")(0),
                  Bytes.toString(result3.value)
                 ))
keyValueRDD3.collect()
//DEFINING THE CASE CLASS FOR HBASE TABLE
case class Song_Artist_Map(songid:String,artistid:String)
//CONVERSION FROM CASE CLASS TO SPARK DATAFRAME
val songartistdf = keyValueRDD3.map{ case (s0,s1)=> Song_Artist_Map(s0,s1)}.toDF()
songartistdf.registerTempTable("Song_Artist_Map")
sqlContext.sql("select * from Song_Artist_Map").show()
songartistdf.show()
//===================================================================
//LOADING HBASE TABLE User_Artist_Map INTO SPARK DATAFRAME

val tableName4 ="User_Artist_Map"
val hconf4 = HBaseConfiguration.create()
hconf4.set(TableInputFormat.INPUT_TABLE, tableName4)
val admin = new HBaseAdmin(hconf4)
val hBaseRDD4 = sc.newAPIHadoopRDD(hconf4, 
classOf[TableInputFormat],
classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], 
classOf[org.apache.hadoop.hbase.client.Result])
val result4 = hBaseRDD4.count()
val resultRDD4 = hBaseRDD4.map(tuple => tuple._2) 
val keyValueRDD4 = resultRDD4.map(result4 =>
                 (Bytes.toString(result4.getRow()).split(" ")(0),
                  Bytes.toString(result4.value)
                 ))
keyValueRDD4.collect()
//DEFINING THE CASE CLASS FOR HBASE TABLE
case class User_Artist_Map(user_id:String,artist_ids:String)
//CONVERSION FROM CASE CLASS TO SPARK DATAFRAME
val userartistsdf = keyValueRDD4.map{ case (s0,s1)=> User_Artist_Map(s0,s1)}.toDF()
userartistsdf.registerTempTable("User_Artist_Map")
sqlContext.sql("select * from User_Artist_Map").show()

//EXPLODING THE SECOND COLUMN WHICH IS ARRAY OF STRINGS INTO STRING

val userartistdf = userartistsdf.withColumn("artist_ids", explode(split($"artist_ids", "[&]")))
userartistdf.show()

//=================================================================
//CREATION OF HIVE TABLE FROM SPARK WHICH CONTAINS DATA FROM BOTH
//THE SOURCES file.txt and file.xml
//
//RUNNING BELOW COMMAND IN ORDER TO USE HIVE WITHIN SPARK
//[cloudera@quickstart ~]$ sudo cp  /usr/lib/hive/conf/hive-site.xml    /usr/lib/spark/conf/
//
//IMPORTING THE PACKAGE TO USE HIVE TABLES AND RUN SPARK SQL ON HIVE TABLES

import org.apache.spark.sql._ 
import org.apache.spark.sql.hive.HiveContext

//Creation of HIVECONTEXT object with the help of SPARK CONTEXT

val sqlContext = new HiveContext(sc)

//CREATION OF DATABASE FROM SPARK SQL

sqlContext.sql("CREATE DATABASE IF NOT EXISTS musicdbb")

//SETTING THE DATABASE IN ORDER TO BROWSE AND CREATING TABLES INSIDE IT

sqlContext.sql("USE musicdbb")

//CREATION OF HIVE TABLE FROM SPARK SQL FOR FINAL EVALUATION

sqlContext.sql("""CREATE TABLE IF NOT EXISTS sparkhivetable(user_id STRING,song_id STRING,artist_id STRING,timestamp STRING,start_ts STRING,end_ts STRING,
geo_cd STRING,station_id STRING,song_end_type INT,like INT,dislike INT)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE""")

//INSERTING DATA OF FILE.TXT AND FILE.XML INTO HIVE TABLE CREATED FROM SPARK SQL

sqlContext.sql("INSERT INTO TABLE sparkhivetable SELECT * FROM musicdba.datasource1") 

//SPARK RDD CREATION FROM THE HIVE TABLE DATA

val datadf1 = sqlContext.sql("SELECT * FROM sparkhivetable").toDF()

//DISPLAY THE CONTENTS OF DATAFRAME

datadf1.show()

//SHOWING THE RECORD COUNT IN DATAFRAME

datadf1.count()

//DISPLAY THE SCHEMA OF DATAFRAME

datadf1.printSchema()

//=====================================================
//----------------DATA ENRICHMENT STEP 1--------------- 

//SETTING LIKE AND DISLIKE TO 0 IF IT IS NULL

val datadf2 = datadf1.select(
$"user_id",
$"song_id",
$"artist_id",
$"timestamp",
$"start_ts",
$"end_ts",
$"geo_cd",
$"station_id",
$"song_end_type",
when(datadf1("like").isNull, 0).otherwise(datadf1("like")).as("like"), 
when(datadf1("dislike").isNull , 0).otherwise(datadf1("dislike")).as("dislike")
)
datadf2.show()
datadf2.printSchema()
datadf2.count()
datadf2.registerTempTable("datadf2table")
sqlContext.sql("select * from datadf2table").show()
sqlContext.sql("select * from datadf2table where user_id  == 'U108' and station_id == 'ST410' and song_id == 'S205'").show()
//=====================================================================
//----------------------DATA ENRICHMENT STEP 2-------------------------

//DECLARING THE BROADCAST TABLE AND DATAFRAME AND PERFORMING LOOKUP FOR GEO_CD IF IT IS NULL OR ABSENT

import org.apache.spark.sql.functions.broadcast 

//BROADCAST JOIN FOR OPTMIZED PERFORMANCE

val sgbroadcast = broadcast(stationgeodf.as("StationGeoMap"))
import sqlContext.implicits._
val datadf3 = datadf2.join(broadcast(sgbroadcast), $"station_id" === $"stationid","leftouter").select(
$"user_id"
,$"song_id"
,$"artist_id"
,$"timestamp"
,$"start_ts",$"end_ts",
when(datadf2("geo_cd").isNull || datadf2("geo_cd") === "", sgbroadcast("geocd").as("geo_cd")).otherwise(datadf2("geo_cd")).as("geo_cd")
,$"station_id"
,$"song_end_type"
,$"like", 
$"dislike")

datadf3.show()
datadf3.printSchema()
datadf3.count()
datadf3.registerTempTable("datadf3table")
sqlContext.sql("select * from datadf3table where user_id  == 'U108' and station_id == 'ST410' and song_id == 'S205'").show()
//============================================================================
//------------------------DATA ENRICHMENT STEP 3------------------------------

//DECLARING THE BROADCAST TABLE AND DATAFRAME AND PERFORMING LOOKUP FOR ARTIST_ID IF IT IS NULL OR ABSENT

//BROADCAST JOIN FOR OPTIMIZED PERFORMANCE

val sabroadcast = broadcast(songartistdf.as("SongArtistMap"))

val datadf4 = datadf3.join(broadcast(sabroadcast), $"song_id" === $"songid","leftouter").select(
$"user_id",
$"song_id",
when(datadf3("artist_id").isNull || datadf3("artist_id") === "", sabroadcast("artistid").as("artist_id")).otherwise(datadf3("artist_id")).as("artist_id"),
$"timestamp",
$"start_ts",
$"end_ts",
$"geo_cd", 
$"station_id",
$"song_end_type",
$"like", 
$"dislike")

datadf4.show()
datadf4.printSchema()
datadf4.count()
datadf4.registerTempTable("datadf4table")
sqlContext.sql("select * from datadf4table where artist_id is null or artist_id == ''").show()


//========================================================================
//VALIDATION OF ENTIRE DATA TO MAKE IT CONSISTENT FOR DATA ANALYSIS AND 
//CREATION OF FINAL SPARK DATAFRAME 



val datadf5 = sqlContext.sql("select * from datadf4table where user_id is not NULL and artist_id is not NULL and song_id is not NULL and like is not NULL and (like==1 or like==0) and dislike is not NULL and (dislike==1 or dislike==0) and geo_cd is not NULL and geo_cd != '' and user_id != '' and artist_id != ''")

datadf5.show()
datadf5.count()
datadf5.printSchema()
datadf5.registerTempTable("datadf5table")
sqlContext.sql("select * from datadf5table").show()

//MAKING THE TIMESTAMP CONSISTENT ALL ACROSS THE DATA IN SPARK DATAFRAME

val datadf6 = datadf5.withColumn("new_timestamp", when( unix_timestamp($"timestamp").isNull , $"timestamp" ).otherwise( unix_timestamp($"timestamp")))
val datadf7 = datadf6.withColumn("new_start_ts", when( unix_timestamp($"start_ts").isNull , $"start_ts" ).otherwise( unix_timestamp($"start_ts")))
val datadf8 = datadf7.withColumn("new_end_ts", when( unix_timestamp($"end_ts").isNull , $"end_ts" ).otherwise( unix_timestamp($"end_ts")))

datadf8.show()
datadf8.count()
datadf8.registerTempTable("datadf8table")
datadf8.printSchema()

//KEEPING ONLY SELECTED ATTRIBUTES ONLY IN FINAL SPARK DATAFRAME FOR DATA ANALYSIS

//val datadf9 = sqlContext.sql("select user_id,song_id,artist_id,new_timestamp,new_start_ts,new_end_ts,geo_cd,station_id,song_end_type,like,dislike from datadf8table")

val datadf9 = datadf8.select($"user_id",$"song_id",$"artist_id",$"new_timestamp",$"new_start_ts",$"new_end_ts",$"geo_cd",$"station_id",$"song_end_type",$"like",$"dislike").filter($"new_start_ts" <= $"new_end_ts")

datadf9.count()
datadf9.printSchema()
datadf9.show()
datadf9.registerTempTable("datadf9table")
sqlContext.sql("select user_id from datadf9table").show()

//==========================================================================
//---------------------------DATA ANALYSIS----------------------------------
//SPARK QUERY 1
//
//Determine top 10 station_id(s) where maximum number of songs were played, which were
//liked by unique users.
//==========================================================================

val sparkdf1 = sqlContext.sql("select station_id,count(*) as total_songs_played_and_liked from datadf9table where like ==1 group by station_id order by total_songs_played_and_liked desc").take(10)

sparkdf1.foreach(println)

//==========================================================================
//SPARK QUERY 2
//
//Determine total duration of songs played by each type of user, where type of user can be
//'subscribed' or 'unsubscribed'. An unsubscribed user is the one whose record is either not
//present in Subscribed_users lookup table or has subscription_end_date earlier than the
//timestamp of the song played by him.
//
//BROADCAST JOIN WITH HBASE TABLE FOR OPTIMIZED PERFORMANCE
//==========================================================================

val subsusersbcast = broadcast(subscribedusersdf.as("SubscribedUsers"))

val datadf10 = datadf9.join(broadcast(subsusersbcast), $"user_id" === $"userid","leftouter").select(
 $"user_id"
,$"song_id"
,$"new_start_ts"
,$"new_end_ts"
,$"subscription_start_date"
,$"subscription_end_date"
)

val datadf11 = datadf10.withColumn(
"user_subscription_status",when(($"subscription_start_date".isNull && $"subscription_end_date".isNull) || $"subscription_end_date" < $"new_end_ts","unsubscribed_users").otherwise("subscribed_users"))

val datadf12 = datadf11.withColumn("duration", $"new_end_ts" - $"new_start_ts")

datadf12.registerTempTable("datadf12table")

//Mentioning the duration in HOURS

val sparkdf2 = sqlContext.sql("select user_subscription_status,((sum(duration)/60)/60) as total_duration_in_hours from datadf12table group by user_subscription_status ")

sparkdf2.show()


//=============================================================
//SPARK QUERY 3
//
//Determine top 10 connected artists. Connected artists are those whose songs are most
//listened by the unique users who follow them.
//BROADCAST JOIN WITH HBASE DATA FOR OPTIMIZED PERFORMANCE
//============================================================

datadf9.registerTempTable("datadf9table")
val uabcast = broadcast(userartistdf.as("UserArtists"))

val sparkdf3 = datadf9.join(broadcast(uabcast),
   datadf9("user_id") === uabcast("user_id")  &&  
   datadf9("artist_id") ===  uabcast("artist_ids")
,"inner").select($"artist_id").distinct.take(10)

sparkdf3.foreach(println)

//==============================================================
//SPARK QUERY 4
//
//Determine top 10 songs who have generated the maximum revenue. Royalty applies to a
//song only if it was liked or was completed successfully or both.
//==============================================================

val sparkdf4 = datadf9.filter($"song_end_type" === 0 || $"like" === 1).groupBy($"song_id").count().sort(desc("count")).take(10)

sparkdf4.foreach(println)

//=================================================================
//SPARK QUERY 5 
//
//Determine top 10 unsubscribed users who listened to the songs for the longest duration.
//=================================================================

//datadf12.show()

val sparkdf5 = datadf12.select($"user_id",$"duration",$"user_subscription_status").filter($"user_subscription_status"==="unsubscribed_users").sort(desc("duration")).take(10)

sparkdf5.foreach(println)


//Exit from spark shell
exit
//=====================================================================
