package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * MutateRowExample Modifies a row with multiple operations
 */
public class MutateRowExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, 3, "colfam1");
        HBaseUtils.put(HBaseConstants.TEST_TABLE,
                new String[]{"row1"},
                new String[]{"colfam1"},
                new String[]{"qual1", "qual2", "qual3"},
                new long[]{1, 2, 3},
                new String[]{"val1", "val2", "val3"});
        System.out.println("Before delete call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
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

            table.mutateRow(mutations);
        }

        System.out.println("After mutate call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);
        HBaseUtils.closeConnection();
    }
}
