package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * GetFluentExample Creates a get request using its fluent interface
 */
public class GetFluentExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", 5, "colfam1", "colfam2");
        helper.put("testtable",
                new String[]{"row1"},
                new String[]{"colfam1", "colfam2"},
                new String[]{"qual1", "qual1", "qual2", "qual2"},
                new long[]{1, 2, 3, 4},
                new String[]{"val1", "val1", "val2", "val2"});
        System.out.println("Before get call...");
        helper.dump("testtable", new String[]{"row1"}, null, null);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        // co GetFluentExample-1-Create Create a new get using the fluent interface.
        Get get = new Get(Bytes.toBytes("row1"))
                .setId("GetFluentExample")
                .readAllVersions()
                .setTimestamp(1)
                .addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
                .addFamily(Bytes.toBytes("colfam2"));

        Result result = table.get(get);
        System.out.println("Result: " + result);

        table.close();
        connection.close();
        helper.close();
    }
}
