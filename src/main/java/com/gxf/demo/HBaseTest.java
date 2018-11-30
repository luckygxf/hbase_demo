package com.gxf.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;


public class HBaseTest {
  private static Configuration conf = null;
  private static Connection connection = null;
  private static Admin admin = null;

  static {
    //设置连接信息
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum","localhost");
    conf.set("hbase.zookeeper.property.clientPort","2181");
    conf.setInt("hbase.rpc.timeout",2000);
    conf.setInt("hbase.client.operation.timeout",3000);
    conf.setInt("hbase.client.scanner.timeout.period",6000);
    try{
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
    }catch (Exception e){
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    String tableName = "test3";
    String[] colFam = new String[]{"colFam"};
    createTable(tableName, colFam);
  }

  /**
   * 创建表
   * */
  public static void createTable(String tableNameStr, String[] colFam){
    try{
      TableName tableName = TableName.valueOf(tableNameStr);
      Table table = connection.getTable(tableName);
      if(admin.tableExists(tableName)){
        //表已经存在
        System.out.println("table is exist: " + tableNameStr);
      }else{
        //表不存在
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableNameStr);
        for(String colStr : colFam){
          HColumnDescriptor columnDescriptor = new HColumnDescriptor(colStr);
          hTableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        System.out.println("creat table success");
        admin.close();
      }
    }catch (Exception e){
      e.printStackTrace();
    }
  }

  /**
   * 删除表
   * 1. disable
   * 2. delete
   * */
  public static void deleteTable(String tableNameStr) throws Exception {
    TableName tableName = TableName.valueOf(tableNameStr);
    if(!admin.tableExists(tableName)){
      System.out.println("");
    }
  }
}