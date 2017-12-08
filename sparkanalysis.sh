# RUNNING HBASESCRIPTS TO CREATE HBASE TABLE VIA COMMAND PROMPT

hbase shell /home/cloudera/chhaya/hbasescripts1.txt

# DELETION OF USER HDFS DIRECTORY IF DOES EXIST

hadoop fs -rm -R /user/cloudera/chhaya/hbasedata/

hadoop fs -rm -R /user/cloudera/chhaya/hivedata/

# CREATION OF HDFS DIRECTORY FOR DIFFERENT FOLDERS FOR HIVE AND HBASE TABLES

hadoop fs -mkdir -p /user/cloudera/chhaya/hbasedata/

hadoop fs -mkdir -p /user/cloudera/chhaya/hivedata/

# LOADING DATA FROM LOCAL FILESYSTEM TO HDFS DIRECTORIES

hadoop fs -copyFromLocal /home/cloudera/Downloads/FinalProject/LookupHBASETables/song-artist.txt /user/cloudera/chhaya/hbasedata/songartist.csv

hadoop fs -copyFromLocal /home/cloudera/Downloads/FinalProject/LookupHBASETables/stn-geocd.txt /user/cloudera/chhaya/hbasedata/stngeocd.csv

hadoop fs -copyFromLocal /home/cloudera/Downloads/FinalProject/LookupHBASETables/user-artist.txt /user/cloudera/chhaya/hbasedata/userartist.csv

hadoop fs -copyFromLocal /home/cloudera/Downloads/FinalProject/LookupHBASETables/user-subscn.txt /user/cloudera/chhaya/hbasedata/usersub.csv

hadoop fs -cat /user/cloudera/chhaya/hbasedata/songartist.csv

hadoop fs -cat /user/cloudera/chhaya/hbasedata/stngeocd.csv

hadoop fs -cat /user/cloudera/chhaya/hbasedata/userartist.csv

hadoop fs -cat /user/cloudera/chhaya/hbasedata/usersub.csv


#LOADING DATA IN CSV FILE FROM HDFS DIRECTORY TO HBASE TABLE

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,subscription_start_date,subscription_end_date '-Dimporttsv.separator=,' Subscribed_Users /user/cloudera/chhaya/hbasedata/usersub.csv

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,artist_id '-Dimporttsv.separator=,' Song_Artist_Map /user/cloudera/chhaya/hbasedata/songartist.csv

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,station_id '-Dimporttsv.separator=,' Station_Geo_Map /user/cloudera/chhaya/hbasedata/stngeocd.csv

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,user_id '-Dimporttsv.separator=,' User_Artist_Map /user/cloudera/chhaya/hbasedata/userartist.csv

#RUNNING HBASE SCRIPTS TO SCAN HBASE TABLES POST LOADING DATA 

hbase shell /home/cloudera/chhaya/hbasescripts2.txt

#LOADING DATA FROM LOCAL DIRECTORY TO HDFS DIRECTORY

hadoop fs -copyFromLocal /home/cloudera/Downloads/FinalProject/Mob_Data_Files/file.txt  /user/cloudera/chhaya/hivedata/

#LOADING DATA FROM LOCAL DIRECTORY TO HDFS DIRECTORY

hadoop fs -copyFromLocal /home/cloudera/Downloads/FinalProject/XML_Data_Files/file.xml  /user/cloudera/chhaya/hivedata/

#BROWSING THE DATA AT HDFS DIRECTORY

hadoop fs -cat /user/cloudera/chhaya/hivedata/file.xml

hadoop fs -cat /user/cloudera/chhaya/hivedata/file.txt

#RUNNING HIVE SCRIPTS FROM HQL FILE VIA COMMAND PROMPT

hive -f /home/cloudera/chhaya/musiclatest.hql

#RUNNING SPARK SCRIPTS FROM SCALA FILE VIA COMMAND PROMPT

spark-shell -i /home/cloudera/chhaya/musiclatest.scala

#END OF ALL SCRIPTS                                                                                                                                            

