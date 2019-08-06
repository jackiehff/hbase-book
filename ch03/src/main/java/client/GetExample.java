package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * GetExample Example application retrieving data from HBase
 */
public class GetExample {

    public static void main(String[] args) throws IOException {
        // co GetExample-1-CreateConf Create the configuration.

        if (!HBaseUtils.existsTable(HBaseConstants.TEST_TABLE)) {
            HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        }
        // co GetExample-2-NewTable Instantiate a new table reference.
        Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);
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
        HBaseUtils.closeConnection();
    }
}
