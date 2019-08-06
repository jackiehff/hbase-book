package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * PutWriteBufferExample2 Example using the client-side write buffer
 */
public class PutWriteBufferExample2 {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");

        //TableName name = TableName.valueOf("testtable");
        Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);
        BufferedMutator mutator = HBaseUtils.getConnection().getBufferedMutator(HBaseConstants.TEST_TABLE);

        // co PutWriteBufferExample2-1-DoPut Create a list to hold all mutations.
        List<Mutation> mutations = new ArrayList<>();

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        // co PutWriteBufferExample2-2-DoPut Add Put instance to list of mutations.
        mutations.add(put1);

        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
        mutations.add(put2);

        Put put3 = new Put(Bytes.toBytes("row3"));
        put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));
        mutations.add(put3);

        // co PutWriteBufferExample2-3-DoPut Store some rows with columns into HBase.
        mutator.mutate(mutations);

        Get get = new Get(Bytes.toBytes("row1"));
        Result res1 = table.get(get);
        // co PutWriteBufferExample2-4-Get1 Try to load previously stored row, this will print "Result: keyvalues=NONE".
        System.out.println("Result: " + res1);
        // co PutWriteBufferExample2-5-Flush Force a flush, this causes an RPC to occur.
        mutator.flush();

        Result res2 = table.get(get);
        // co PutWriteBufferExample2-6-Get2 Now the row is persisted and can be loaded.
        System.out.println("Result: " + res2);

        mutator.close();
        table.close();
        HBaseUtils.closeConnection();
    }
}
