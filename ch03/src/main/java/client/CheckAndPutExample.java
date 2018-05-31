package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * CheckAndPutExample Example application using the atomic compare-and-set operations
 */
public class CheckAndPutExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        // Create a new Put instance.
        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));

        // Check if column does not exist and perform optional put operation.
        boolean res1 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"))
                .qualifier(Bytes.toBytes("qual1"))
                .ifNotExists()
                .thenPut(put1);
        // Print out the result, should be "Put 1a applied: true".
        System.out.println("Put 1a applied: " + res1);

        // Attempt to store same cell again.
        boolean res2 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"))
                .qualifier(Bytes.toBytes("qual1"))
                .ifNotExists().thenPut(put1);
        // Print out the result, should be "Put 1b applied: false" as the column now already exists.
        System.out.println("Put 1b applied: " + res2);

        // Create another Put instance, but using a different column qualifier.
        Put put2 = new Put(Bytes.toBytes("row1"));
        put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));

        // co CheckAndPutExample-07-CAS3 Store new data only if the previous data has been saved.
        boolean res3 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"))
                .qualifier(Bytes.toBytes("qual1"))
                .ifEquals(Bytes.toBytes("val1"))
                .thenPut(put2);
        // Print out the result, should be "Put 2 applied: true" as the checked column exists.
        System.out.println("Put 2 applied: " + res3);

        // Create yet another Put instance, but using a different row.
        Put put3 = new Put(Bytes.toBytes("row2"));
        put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));

        // Store new data while checking a different row.
        boolean res4 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"))
                .qualifier(Bytes.toBytes("qual1"))
                .ifEquals(Bytes.toBytes("val1"))
                .thenPut(put3);
        // We will not get here as an exception is thrown beforehand!
        System.out.println("Put 3 applied: " + res4);
        table.close();
        connection.close();
        helper.close();
    }
}
