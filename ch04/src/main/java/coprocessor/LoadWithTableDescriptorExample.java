package coprocessor;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * LoadWithTableDescriptorExample Load a coprocessor using the table descriptor
 */
public class LoadWithTableDescriptorExample {

    public static void main(String[] args) throws IOException {
        TableName tableName = TableName.valueOf("testtable");
        HBaseUtils.dropTable(tableName);

        // Define a table descriptor, Add the coprocessor definition to the descriptor, while omitting the path to the JAR file
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .setValue("COPROCESSOR$1", "|" + RegionObserverExample.class.getCanonicalName() + "|" + Coprocessor.PRIORITY_USER).build();

        // Acquire an administrative API to the cluster and add the table.
        Admin admin = HBaseUtils.getConnection().getAdmin();
        admin.createTable(tableDescriptor);

        // Verify if the definition has been applied as expected.
        System.out.println(admin.getDescriptor(tableName));
        admin.close();
        HBaseUtils.closeConnection();
    }
}