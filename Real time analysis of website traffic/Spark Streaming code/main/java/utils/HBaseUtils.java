package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtils {


    private HBaseConfiguration conf;

    /**
     * private constructor
     */
    private HBaseUtils(){
        conf=new HBaseConfiguration();

        conf.set("hbase.zookeeper.quorum", "hadoop132");
        conf.set("hbase.rootdir","hdfs://hadoop132:8082/habse");

    }

    private  static HBaseUtils instance=null;

    //单例
    public static synchronized HBaseUtils getInstance(){
        if(null==instance){
            instance=new HBaseUtils();
        }
        return instance;
    }

    /***
     * get the table
     * @param tableName  the table name
     * @return
     */
    public HTable getTable(String tableName){
        HTable table=null;

        try {
            table=new HTable(conf,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * put the data in table
     * @param tableName
     * @param rowKey
     * @param cf column family
     * @param column column name in column family
     * @param value
     */
    public void put(String tableName,String rowKey,String cf,String column,long value){
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));

        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        HTable table = HBaseUtils.getInstance().getTable("course_clickcount");
//        System.out.println(table.getName());
        String tableName="course_clickcount";
        String rowKey="20191111_90";
        String cf="info";
        String column="click_count";
        long value=67;

        HBaseUtils.getInstance().put(tableName,rowKey,cf,column,value);

    }

}
