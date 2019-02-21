package client;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * PutListErrorExample2 Example inserting an empty Put instance into HBase
 */
public class PutListErrorExample2 {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");

        Table table = helper.getTable("testtable");

        List<Put> puts = new ArrayList<>();

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        puts.add(put1);
        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("BOGUS"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
        puts.add(put2);
        Put put3 = new Put(Bytes.toBytes("row2"));
        put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val3"));
        puts.add(put3);
        Put put4 = new Put(Bytes.toBytes("row2"));
        // co PutListErrorExample2-1-AddErrorPut Add put with no content at all to list.
        puts.add(put4);

        try {
            table.put(puts);
        } catch (Exception e) {
            System.err.println("Error: " + e);
            // table.flushCommits();
            // todo: FIX!
            // co PutListErrorExample2-2-Catch Catch local exception and commit queued updates.
        }
        table.close();
        helper.close();
    }
}
