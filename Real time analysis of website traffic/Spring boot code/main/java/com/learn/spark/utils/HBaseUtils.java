package com.learn.spark.utils;

import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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


    public Map<String,Long> quary(String tableName,String day)throws Exception{
        Map<String,Long> map=new HashMap<>();

        HTable table = getTable(tableName);
        String cf="info";
        String qualifier="click_count";
        Scan scan = new Scan();
        PrefixFilter filter = new PrefixFilter(Bytes.toBytes(day));
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for(Result result:rs){
            // row is the rowkey
            String row = Bytes.toString(result.getRow());
            long clickCount = Bytes.toLong(result.getValue(cf.getBytes(), qualifier.getBytes()));
            map.put(row,clickCount);
        }
        // return the map {row_key:click_count}
        return map;
    }


    public static void main(String[] args) throws Exception{
//        HTable table = HBaseUtils.getInstance().getTable("course_clickcount");
//        System.out.println(table.getName());

        Map<String, Long> map = HBaseUtils.getInstance().quary("course_clickcount", "00191213");

        for(Map.Entry<String,Long> entry:map.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }


    }

}
