package com.gxf.demo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseTest {

  private static Configuration conf = null;
  private static Connection connection = null;
  private static Admin admin = null;
  private static Logger logger = LoggerFactory.getLogger(HBaseTest.class);

  static {
    //设置连接信息
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.setInt("hbase.rpc.timeout", 2000);
    conf.setInt("hbase.client.operation.timeout", 3000);
    conf.setInt("hbase.client.scanner.timeout.period", 6000);
    try {
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    String tableName = "test3";
    String[] colFam = new String[]{"colFam"};
//    createTable(tableName, colFam);
//    deleteTable(tableName);
//    listTables();
//    addData("users", "row3", "info", "name", "guanxianseng");
//    deleteData("users", "row1", "info", "name");
//    query("users", "row2", "info", "name");
    scan("users", "row1", "row2");
  }

  /**
   * scan数据
   * */
  public static void scan(String tableNameStr, String startRowKey, String stopRowKey)
      throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableNameStr));
    Scan scan = new Scan();
    ResultScanner resultScanner = table.getScanner(scan);
    for(Result result : resultScanner){
      showCell(result);
    }
  }

  /**
   * 查询数据
   * */
  public static void query(String tableNameStr, String rowkey, String colFam, String col)
      throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableNameStr));
    Get get = new Get(rowkey.getBytes());
    Result result = table.get(get);
    showCell(result);
  }

  /**
   * 打印一个cell所有数据
   * */
  private static void showCell(Result result){
    for(Cell cell : result.rawCells()){
      logger.info("rawname:{}, timestamp:{}, colFam:{}, colName:{}, value:{}", new String(CellUtil.cloneRow(cell)), cell.getTimestamp(),
                  new String(CellUtil.cloneFamily(cell)), new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
    }
  }

  /**
   * 删除数据
   * */
  public static void deleteData(String tableNameStr, String row,  String colFam, String col) throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableNameStr));
    Delete delete = new Delete(row.getBytes());
    table.delete(delete);
    logger.info("delete tablename: {}, row:{}, colFam:{}, col:{}", tableNameStr, row, colFam, col);
  }

  /**
   * 向表中插入数据
   * */
  public static void addData(String tableNameStr, String rowkey, String colFam, String col, String value)
      throws IOException {
    TableName tableName = TableName.valueOf(tableNameStr);
    Table table = connection.getTable(tableName);
    Put put = new Put(rowkey.getBytes());
    put.addColumn(colFam.getBytes(), col.getBytes(), value.getBytes());
    table.put(put);
    table.close();
    logger.info("put table:{}, rowkey:{}, colFam:{}, col:{}, value:{}", tableNameStr, rowkey, colFam, col, value);
  }

  /**
   * 列出所有的表
   * */
  public static void listTables() throws IOException {
    HTableDescriptor[] hTableDescriptors = admin.listTables();
    for(HTableDescriptor hTableDescriptor : hTableDescriptors){
      logger.info("table :{}", hTableDescriptor.getTableName());
    }
  }

  /**
   * 创建表
   */
  public static void createTable(String tableNameStr, String[] colFam) {
    try {
      TableName tableName = TableName.valueOf(tableNameStr);
      Table table = connection.getTable(tableName);
      if (admin.tableExists(tableName)) {
        //表已经存在
        logger.info("table {} already exist", tableNameStr);
      } else {
        //表不存在
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableNameStr);
        for (String colStr : colFam) {
          HColumnDescriptor columnDescriptor = new HColumnDescriptor(colStr);
          hTableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        logger.info("creat table success");
        admin.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 删除表 1. disable 2. delete
   */
  public static void deleteTable(String tableNameStr) throws Exception {
    TableName tableName = TableName.valueOf(tableNameStr);
    if (!admin.tableExists(tableName)) {
      logger.error("table :{} not exist", tableNameStr);
    } else {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      logger.info("delete table:{}", tableNameStr);
    }
  }
}