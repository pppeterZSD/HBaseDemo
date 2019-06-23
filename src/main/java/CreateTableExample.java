import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class CreateTableExample {

    private static final String TABLE_NAME = "test_12";
    private static final String COLUMN_FAMILY_NAME = "cf";
    private static final String RemoteHost = "10.21.4.133";

    public static void main(String[] args) throws IOException {

        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum",RemoteHost);
        conf.set("hbase.zookeeper.property.clientPort","2181");

        CreateTableExample createTableExample = new CreateTableExample();


        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            createTableExample.listTables(admin);
            createTableExample.createTable(admin);

        }catch (IOException e){
            e.printStackTrace();
        }

    }

    private void createTable (Admin admin) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        System.out.println("To create table named " + TABLE_NAME);
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

