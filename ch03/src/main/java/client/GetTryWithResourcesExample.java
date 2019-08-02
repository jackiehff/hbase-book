package client;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * GetTryWithResourcesExample Example application retrieving data from HBase using a Java 7 construct
 */
public class GetTryWithResourcesExample {

    public static void main(String[] args) throws IOException {
        // co GetTryWithResourcesExample-1-CreateConf Create the configuration.

        if (!HBaseUtils.existsTable("testtable")) {
            HBaseUtils.createTable("testtable", "colfam1");
        }

        try (
                // co GetTryWithResourcesExample-2-NewTable Instantiate a new table reference in "try" block.
                Table table = HBaseUtils.getTable("testtable")
        ) {
            Get get = new Get(Bytes.toBytes("row1"));
            get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
            Result result = table.get(get);
            byte[] val = result.getValue(Bytes.toBytes("colfam1"),
                    Bytes.toBytes("qual1"));
            System.out.println("Value: " + Bytes.toString(val));
        }// co GetTryWithResourcesExample-3-Close No explicit close needed, Java will handle AutoClosable's.
        HBaseUtils.closeConnection();
    }
}
