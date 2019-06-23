import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 测试 HBase 能否通过java远程访问
 */

public class HBaseTest {
    private static final String TABLE_NAME = "vnptest";
    private static final String COLUMN_FAMILY_NAME = "cf";

    public static void main(String[] args) throws IOException {

//        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum","192.168.128.111");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        HBaseTest HBaseTest = new HBaseTest();


        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            HBaseTest.listTables(admin);
            HBaseTest.createTable(admin);

        }catch (IOException e){
            e.printStackTrace();
        }

    }

    private void createTable (Admin admin) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        System.out.print("To create table named " + TABLE_NAME);
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor columnDesc = new HColumnDescriptor(COLUMN_FAMILY_NAME);
        tableDesc.addFamily(columnDesc);

        admin.createTable(tableDesc);
    }

    /**
     * 列出所有表
     * @param admin
     * @throws IOException
     */

    private void listTables (Admin admin) throws IOException {
        TableName[] names = admin.listTableNames();
        for (TableName tableName : names) {
            System.out.println("Table Name is : " + tableName.getNameAsString());
        }
    }

}