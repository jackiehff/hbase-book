package client;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * PutIdenticalExample Example adding an identical column twice
 */
public class PutIdenticalExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");

        Table table = helper.getTable("testtable");

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
        helper.close();
    }
}
