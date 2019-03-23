package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TableInformation {

  public static final String TABLE_NAME = "blog";
  public static final String FAMILY_NAME_1 = "article";
  public static final String FAMILY_NAME_2 = "author";
  public static final String QUALIFIER_NAME_1_1 = "title";
  public static final String QUALIFIER_NAME_1_2 = "content";
  public static final String QUALIFIER_NAME_1_3 = "tag";
  public static final String QUALIFIER_NAME_2_1 = "name";
  public static final String QUALIFIER_NAME_2_2 = "gender";
  public static final String QUALIFIER_NAME_2_3 = "age";

  public static Configuration getHBaseConfiguration() {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum",
        "localhost");
    conf.set("zookeeper.znode.parent", "/hbase");

    return conf;
  }

  public static void createTable(Configuration conf) throws IOException {
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
      HColumnDescriptor columnDescriptor_1 = new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME_1));
      HColumnDescriptor columnDescriptor_2 = new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME_2));
      tableDescriptor.addFamily(columnDescriptor_1);
      tableDescriptor.addFamily(columnDescriptor_2);
      admin.createTable(tableDescriptor);
    }
  }

  public static void deleteTable(Configuration conf) throws IOException {
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
      if (!admin.isTableDisabled(TableName.valueOf(TABLE_NAME))) {
        admin.disableTable(TableName.valueOf(TABLE_NAME));
      }
      admin.deleteTable(TableName.valueOf(TABLE_NAME));
    }
  }

  public static void main(String args[]) throws IOException {
    TableInformation.createTable(TableInformation.getHBaseConfiguration());
  }

}

