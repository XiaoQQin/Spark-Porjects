This demo is expansion of [spark-streaming-flume-kafka-integration](https://github.com/XiaoQQin/spark_streaming-flume-kafka-integration).
Compared to before, it stores the data in the **Hbase** and uses **Spring boot** for visualization.  
![p_1](../pictures/website_traffic_2.PNG#pic_center)  

![p_2](../pictures/website_traffic_3.PNG#pic_center)

## version
apache-flume-1.7.0  
jdk-8u144-linux-x64  
kafka_2.11-0.11.0.2  
zookeeper-3.4.10  
spark-2.1.1  
hbase-1.3.1  
echarts-4.5.0  
jquery-3.4.1

## process
1. start kafka,hbase,zookeeper,hdfs
   ```
   cd $ZOOKEEPER_HOME
   bin/zkServer.sh start
   
   cd $KAFKA_HOME
   bin/kafka-server-start.sh -daemon /config/server.properties
   
   cd $HADOOP_HOME
   sbin/start-dfs.sh
   
   cd $HBASE_HOME
   bin/start-hbase.sh
   ```
2. create hbase datasets
   ```
   cd $HBASE_HOME
   bin/hbase shell
   create 'course_clickcount','info'
   create 'course_search_clickcount','info'
   ```
3. create Kafka topic name streamingtopic
   ```
   bin/kafka-topics.sh --create --zookeeper hadoop132:2181 --replication-factor 1 --partitions 1 --topic streamingtopic
   ```
   if your Kakfa version is >2.2.x , you should use --bootstrap-server replace the --zookeeper.you can see [kafka documenation](http://kafka.apache.org/documentation/) for detail.
  
  
4. start log_generator.sh  to create log
   ```
   sh log_generator.sh test.log
   ```
   the **log_generator.sh** is the shell file,it use the **generate_log.py** to  generate logs every 60 seconds.Note the log_generator.sh
   and generate_log.py must be in the same directory. **test.log** is the filename of logs,you can define by yourself.    
   
   the style of logs as followed:  
   ```
   ip local_time \"GET /url HTTP/1.1\" referer status_code
   ```
5. create Flume agent file name streaming_kafka.conf  
   the streaming.conf should be added in the dir of flume.Note,if you change the filename of logs,you must change the next line in  streaming.conf.  
     ```
     exec-momory-kafka.sources.exec-source.command=tail -F /opt/module/spark/test.log
     ```
    And you also must change the kafka.bootstrap.servers as followed:
    ```
    exec-momory-kafka.sinks.kafka-sink.kafka.bootstrap.servers = hadoop132:9092,hadoop133:9092,hadoop134:9092
    ```
    then start the exec-momory-kafka
    ```
    bin/flume-ng agent \
    --conf conf \
    --conf-file streaming.conf \
    --name exec-momory-kafka \
    -Dflume.root.logger=INFO,console
    ```
 6. start StatusStreamingApp.scala   
    start **StatusStreamingApp.scala** in Spark Streaming dir to analyze the logs and store the data in hbase.  you must change the hostname to your owm. 
    ```
    bootstrap.servers" -> "hadoop132:9092"
    ```
    you can use the hbase shell to see whether the data was successfully stored
    ```
    cd $HBASE_HOME
    bin/hbase shell
    scan 'course_clickcount'
    scan 'course_search_clickcount'
    ```
 7. start WebApplication 
    start **WebApplication.java** in Spring boot dir to for data visualization.
    Finally you can visit the 'localhost:8080/echarts' to see the result as the pictures showed
