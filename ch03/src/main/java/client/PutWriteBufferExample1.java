package client;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * PutWriteBufferExample1 Example using the client-side write buffer
 */
public class PutWriteBufferExample1 {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");

        TableName name = TableName.valueOf("testtable");
        Connection connection = helper.getConnection();
        Table table = connection.getTable(name);
        // co PutWriteBufferExample1-1-CheckFlush Get a mutator instance for the table.
        BufferedMutator mutator = connection.getBufferedMutator(name);

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        // co PutWriteBufferExample1-2-DoPut Store some rows with columns into HBase.
        mutator.mutate(put1);

        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
        mutator.mutate(put2);

        Put put3 = new Put(Bytes.toBytes("row3"));
        put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));
        mutator.mutate(put3);

        Get get = new Get(Bytes.toBytes("row1"));
        Result res1 = table.get(get);
        // co PutWriteBufferExample1-3-Get1 Try to load previously stored row, this will print "Result: keyvalues=NONE".
        System.out.println("Result: " + res1);

        // co PutWriteBufferExample1-4-Flush Force a flush, this causes an RPC to occur.
        mutator.flush();

        Result res2 = table.get(get);
        // co PutWriteBufferExample1-5-Get2 Now the row is persisted and can be loaded.
        System.out.println("Result: " + res2);

        mutator.close();
        table.close();
        connection.close();
        helper.close();
    }
}
