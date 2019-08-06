package client;


import constant.HBaseConstants;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * CheckAndMutateExample Example using the atomic check-and-mutate operations
 */
public class CheckAndMutateExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, 100, "colfam1", "colfam2");
        HBaseUtils.put(HBaseConstants.TEST_TABLE,
                new String[]{"row1"},
                new String[]{"colfam1", "colfam2"},
                new String[]{"qual1", "qual2", "qual3"},
                new long[]{1, 2, 3},
                new String[]{"val1", "val2", "val3"});
        System.out.println("Before check and mutate calls...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);

        Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);

        //BinaryComparator bc = new BinaryComparator(Bytes.toBytes("val1"));
        //System.out.println(bc.compareTo(Bytes.toBytes("val2")));

        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                4, Bytes.toBytes("val99"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual4"),
                4, Bytes.toBytes("val100"));

        Delete delete = new Delete(Bytes.toBytes("row1"));
        delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));

        RowMutations mutations = new RowMutations(Bytes.toBytes("row1"));
        mutations.add((Mutation) put);
        mutations.add((Mutation) delete);

        // Check if the column contains a value that is less than "val1". Here we receive "false" as the value is equal, but not lesser.
        boolean res1 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam2"))
                .qualifier(Bytes.toBytes("qual1"))
                .ifMatches(CompareOperator.LESS, Bytes.toBytes("val1"))
                .thenMutate(mutations);
        System.out.println("Mutate 1 successful: " + res1);

        Put put2 = new Put(Bytes.toBytes("row1"));
        // Update the checked column to have a value greater than what we check for.
        put2.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"),
                4, Bytes.toBytes("val2"));
        table.put(put2);

        // Now "val1" is less than "val2" (binary comparison) and we expect "true" to be printed on the console.
        boolean res2 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam2"))
                .qualifier(Bytes.toBytes("qual1"))
                .ifMatches(CompareOperator.LESS, Bytes.toBytes("val1"))
                .thenMutate(mutations);
        System.out.println("Mutate 2 successful: " + res2);

        System.out.println("After check and mutate calls...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);
        table.close();
        HBaseUtils.closeConnection();
    }
}
