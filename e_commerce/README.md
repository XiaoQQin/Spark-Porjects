This demo simulates the analysis of e-commerce website data,use the spark to analyze the local and real-time data separately.finally,the result will be stored into the MySQL.

# code structure
The code structure of this project as follows,there are three modules.
![code structure](/pictures/e_commerce_structure.png)  

**analysis**: The main modules of this project,including the operations of local data and real-time data analysis.  
1. **adStat**: use the spark streaming to analyze the ad click real-time data.
2. **area**: analyze the local data to get the result of different regions,such as the popular product.
3. **page**: analyze the local data to get the jump rate between pages.
4. **session**: analyze the local data to get the statistics of user access sessions.

**commons**: the public module of this project,including  tool classes of configuration and MySQL.  
**generateData**:  This module is used to generate data，including local data and real-time data.
**sql**: there are some sql files in this directory,you must use these sql files to create MySQL tables before running the code.
# data format
There are two files in the **generateData** module,**VisitDataGenerator.scala** and **RealTimeDataGenerator.scala**.The first file generate the local data,including userInfo,productInfo and userVisitAction.The data format of userVisitAction is shown below.
```
case class UserVisitAction(
                        date:String,                  //yyyy-MM-dd
                        user_id:Long,                 
                        session_id:String,            //The id of the session
                        page_id:Long,                 //page ID
                        action_time:String,           //the time of action
                        search_keyword:String,        
                        click_category_id: Long,     
                        click_product_id: Long,      
                        order_category_ids: String,  
                        order_product_ids: String,    
                        pay_category_ids: String,     
                        pay_product_ids: String,      
                        city_id: Long                 
                        )
```
There are four actions in UserVisitAction,including search,click,order and pay.Each data contains only one action,the values of other actions will be null or -1.The one example as follows,the action in this data is order.
```
2020-02-21,52,5b057f94-64ac-48fb-9393-69bb73612756,3,2020-02-21 11:08:11,null,-1,-1,18,74,null,null,3
```
The data formats of userInfo and productInfo are showed bellow.
```
case class UserInfo(user_id: Long,                  
                username: String,                   
                name: String,                       
                age: Int,                           
                professional: String,               
                city: String,                       
                sex: String)                        

case class ProductInfo(product_id: Long,            
                   product_name: String,             
                   extend_info: String              
                  )

```
# note
If you want to run the **adStat** module,you must run the **zookeeper** and **kafka** firstly.The commplete process as follows.
1. start zookeeper and kafka
```
cd $ZOOKEEPER_HOME
bin/zkServer.sh start

cd $KAFKA_HOME
bin/kafka-server-start.sh -daemon /config/server.properties

```
2. create kafka topic
```
bin/kafka-topics.sh --create --zookeeper hadoop132:2181 --replication-factor 1 --partitions 1 --topic AdRealTimeLog1
```
the topic in the project is 'AdRealTimeLog1',you can rename it by yourself.    

3.  run RealTimeDataGenerator.scala  
4.  run adStat module
