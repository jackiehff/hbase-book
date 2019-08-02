package client;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * PutListExample Example inserting data into HBase using a list
 */
public class PutListExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable("testtable");
        HBaseUtils.createTable("testtable", "colfam1");

        Table table = HBaseUtils.getTable("testtable");

        // co PutListExample-1-CreateList Create a list that holds the Put instances.
        List<Put> puts = new ArrayList<>();

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        // co PutListExample-2-AddPut1 Add put to list.
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
        // co PutListExample-3-AddPut2 Add another put to list.
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("row2"));
        put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val3"));
        // co PutListExample-4-AddPut3 Add third put to list.
        puts.add(put3);

        // co PutListExample-5-DoPut Store multiple rows with columns into HBase.
        table.put(puts);
        table.close();
        HBaseUtils.closeConnection();
    }
}
