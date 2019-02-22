package coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import util.HBaseHelper;

import java.io.IOException;

/**
 * LoadWithTableDescriptorExample2 Load a coprocessor using the table descriptor using provided method
 */
public class LoadWithTableDescriptorExample2 {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        TableName tableName = TableName.valueOf("testtable");

        // co LoadWithTableDescriptorExample2-1-Create Use fluent interface to create and configure the instance.
        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor("colfam1"));
        // co LoadWithTableDescriptorExample2-2-AddCP Use the provided method to add the coprocessor.
        htd.addCoprocessor(RegionObserverExample.class.getCanonicalName(), null, Coprocessor.PRIORITY_USER, null);

        Admin admin = helper.getConnection().getAdmin();
        admin.createTable(htd);

        System.out.println(admin.getDescriptor(tableName));
        admin.close();
        helper.close();
    }
}
