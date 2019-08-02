package admin;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * TableOperationsExample Example using the various calls to disable, enable, and check that status of a table
 */
public class TableOperationsExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);

        Admin admin = HBaseUtils.getConnection().getAdmin();
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(HBaseConstants.TEST_TABLE)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .build();
        System.out.println("Creating table...");
        admin.createTable(tableDescriptor);

        System.out.println("Deleting enabled table...");
        try {
            admin.deleteTable(HBaseConstants.TEST_TABLE);
        } catch (IOException e) {
            System.err.println("Error deleting table: " + e.getMessage());
        }

        System.out.println("Disabling table...");
        admin.disableTable(HBaseConstants.TEST_TABLE);
        boolean isDisabled = admin.isTableDisabled(HBaseConstants.TEST_TABLE);
        System.out.println("Table is disabled: " + isDisabled);

        boolean avail1 = admin.isTableAvailable(HBaseConstants.TEST_TABLE);
        System.out.println("Table available: " + avail1);

        System.out.println("Deleting disabled table...");
        admin.deleteTable(HBaseConstants.TEST_TABLE);

        boolean avail2 = admin.isTableAvailable(HBaseConstants.TEST_TABLE);
        System.out.println("Table available: " + avail2);

        System.out.println("Creating table again...");
        admin.createTable(tableDescriptor);
        boolean isEnabled = admin.isTableEnabled(HBaseConstants.TEST_TABLE);
        System.out.println("Table is enabled: " + isEnabled);
    }
}
