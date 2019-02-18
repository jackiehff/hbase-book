package coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import util.HBaseHelper;

import java.io.IOException;

/**
 * LoadWithTableDescriptorExample Load a coprocessor using the table descriptor
 */
public class LoadWithTableDescriptorExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        TableName tableName = TableName.valueOf("testtable");

        // co LoadWithTableDescriptorExample-1-Define Define a table descriptor.
        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor("colfam1"));
        // co LoadWithTableDescriptorExample-2-AddCP Add the coprocessor definition to the descriptor, while omitting the path to the JAR file.
        htd.setValue("COPROCESSOR$1", "|" + RegionObserverExample.class.getCanonicalName() + "|" + Coprocessor.PRIORITY_USER);

        // co LoadWithTableDescriptorExample-3-Admin Acquire an administrative API to the cluster and add the table.
        Admin admin = connection.getAdmin();
        admin.createTable(htd);

        // co LoadWithTableDescriptorExample-4-Check Verify if the definition has been applied as expected.
        System.out.println(admin.getDescriptor(tableName));
        admin.close();
        connection.close();
    }
}