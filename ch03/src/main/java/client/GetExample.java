package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * GetExample Example application retrieving data from HBase
 */
public class GetExample {

    public static void main(String[] args) throws IOException {
        // co GetExample-1-CreateConf Create the configuration.
        // Configuration conf = HBaseConfiguration.create();
        // HBaseHelper helper = HBaseHelper.getHelper(conf);
        HBaseHelper helper = HBaseHelper.getHelper();
        if (!helper.existsTable("testtable")) {
            helper.createTable("testtable", "colfam1");
        }
        // Connection connection = ConnectionFactory.createConnection(conf);
        Connection connection = helper.getConnection();
        // co GetExample-2-NewTable Instantiate a new table reference.
        Table table = connection.getTable(TableName.valueOf("testtable"));
        // co GetExample-3-NewGet Create get with specific row.
        Get get = new Get(Bytes.toBytes("row1"));
        // co GetExample-4-AddCol Add a column to the get.
        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        // co GetExample-5-DoGet Retrieve row with selected columns from HBase.
        Result result = table.get(get);

        // co GetExample-6-GetValue Get a specific value for the given column.
        byte[] val = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        // co GetExample-7-Print Print out the value while converting it back.
        System.out.println("Value: " + Bytes.toString(val));

        // co GetExample-8-Close Close the table and connection instances to free resources.
        table.close();
        connection.close();
        helper.close();
    }
}
