package admin;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * CreateTableExample Example using the administrative API to create a table
 */
public class CreateTableExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        // co CreateTableExample-1-CreateAdmin Create a administrative API instance.
        Admin admin = helper.getConnection().getAdmin();

        TableName tableName = TableName.valueOf("testtable");
        // Create the table descriptor instance.
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .build();

        // Call the createTable() method to do the actual work.
        admin.createTable(tableDescriptor);

        // co CreateTableExample-5-Check Check if the table is available.
        boolean avail = admin.isTableAvailable(tableName);
        System.out.println("Table available: " + avail);
    }
}
