package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * GetTryWithResourcesExample Example application retrieving data from HBase using a Java 7 construct
 */
public class GetTryWithResourcesExample {

    public static void main(String[] args) throws IOException {
        // co GetTryWithResourcesExample-1-CreateConf Create the configuration.
        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        if (!helper.existsTable("testtable")) {
            helper.createTable("testtable", "colfam1");
        }

        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                // co GetTryWithResourcesExample-2-NewTable Instantiate a new table reference in "try" block.
                Table table = connection.getTable(TableName.valueOf("testtable"))
        ) {
            Get get = new Get(Bytes.toBytes("row1"));
            get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
            Result result = table.get(get);
            byte[] val = result.getValue(Bytes.toBytes("colfam1"),
                    Bytes.toBytes("qual1"));
            System.out.println("Value: " + Bytes.toString(val));
        }// co GetTryWithResourcesExample-3-Close No explicit close needed, Java will handle AutoClosable's.
        helper.close();
    }
}
