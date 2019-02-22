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
 * TableOperationsExample Example using the various calls to disable, enable, and check that status of a table
 */
public class TableOperationsExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");

        Admin admin = helper.getConnection().getAdmin();

        TableName tableName = TableName.valueOf("testtable");
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .build();
        System.out.println("Creating table...");
        admin.createTable(tableDescriptor);

        System.out.println("Deleting enabled table...");
        try {
            admin.deleteTable(tableName);
        } catch (IOException e) {
            System.err.println("Error deleting table: " + e.getMessage());
        }

        System.out.println("Disabling table...");
        admin.disableTable(tableName);
        boolean isDisabled = admin.isTableDisabled(tableName);
        System.out.println("Table is disabled: " + isDisabled);

        boolean avail1 = admin.isTableAvailable(tableName);
        System.out.println("Table available: " + avail1);

        System.out.println("Deleting disabled table...");
        admin.deleteTable(tableName);

        boolean avail2 = admin.isTableAvailable(tableName);
        System.out.println("Table available: " + avail2);

        System.out.println("Creating table again...");
        admin.createTable(tableDescriptor);
        boolean isEnabled = admin.isTableEnabled(tableName);
        System.out.println("Table is enabled: " + isEnabled);
    }
}
