# Spark-Porjects
These are some demos about the Spark I wrote during my study.

## 1.[website traffic analysis](https://github.com/XiaoQQin/Spark-Porjects/tree/master/Real%20time%20analysis%20of%20website%20traffic)
This demo integrates the **Spark Streaming, Flume, Kafka, Hbase and Spring Boot**. 
It uses the python file to generate logs of the website, analyzing logs by **Spark Streaming** and store the data into **Hbase**, 
finally using **Sping boot** to create a simple HTML to show the data. Data is visualized to view more easily.
  
The  flow of the program is as follows：
 ![Alt](/pictures/website_traffic.PNG#pic_center)
   
You can see [Real time analysis of website traffic](https://github.com/XiaoQQin/Spark-Porjects/tree/master/Real%20time%20analysis%20of%20website%20traffic) for more detail
  
 
## 2. [e-commerce data analysis](https://github.com/XiaoQQin/Spark-Porjects/tree/master/e_commerce)
This demo simulates the analysis of e-commerce website data. The website data is divided into local data and real-time data,using **spark core** and **spark streaming** to analyze them respectively.Finally, the result is stored into **mysql**.
  
The  flow of the program is as follows：
![Alt](/pictures/e_commerce.PNG)

You can see [e_commerce date analysis](https://github.com/XiaoQQin/Spark-Porjects/tree/master/e_commerce) for more detail
