package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * PutIdenticalExample Example adding an identical column twice
 */
public class PutIdenticalExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
        // co PutIdenticalExample-1-Add Add the same column with a different value. The last value is going to be used.
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        table.put(put);

        Get get = new Get(Bytes.toBytes("row1"));
        Result result = table.get(get);
        // co PutIdenticalExample-2-Get Perform a get to verify that "val1" was actually stored.
        System.out.println("Result: " + result + ", Value: " + Bytes.toString(
                result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))));

        table.close();
        connection.close();
        helper.close();
    }
}
