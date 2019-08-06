package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * PutExample Example application inserting data into HBase
 */
public class PutExample {

    public static void main(String[] args) throws IOException {
        // Create the required configuration.
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            // Create put with specific row.
            Put put = new Put(Bytes.toBytes("row1"));
            // Add a column, whose name is "colfam1:qual1", to the put.
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
            // Add another column, whose name is "colfam1:qual2", to the put.
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));

            // Store row with column into the HBase table.
            table.put(put);
        }

        HBaseUtils.closeConnection();
    }
}