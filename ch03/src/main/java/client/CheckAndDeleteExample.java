package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * CheckAndDeleteExample Example application using the atomic compare-and-set operations
 */
public class CheckAndDeleteExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, 100, "colfam1", "colfam2");
        HBaseUtils.put(HBaseConstants.TEST_TABLE,
                new String[]{"row1"},
                new String[]{"colfam1", "colfam2"},
                new String[]{"qual1", "qual2", "qual3"},
                new long[]{1, 2, 3},
                new String[]{"val1", "val2", "val3"});
        System.out.println("Before delete call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);

        Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);

        Delete delete1 = new Delete(Bytes.toBytes("row1"));
        // Create a new Delete instance.
        delete1.addColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"));

        // Check if column does not exist and perform optional delete operation.
        boolean res1 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam2"))
                .qualifier(Bytes.toBytes("qual3")).ifEquals(null).thenDelete(delete1);
        // Print out the result, should be "Delete successful: false".
        System.out.println("Delete 1 successful: " + res1);

        Delete delete2 = new Delete(Bytes.toBytes("row1"));
        // Delete checked column manually.
        delete2.addColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"));
        table.delete(delete2);

        // co CheckAndDeleteExample-5-CAS2 Attempt to delete same cell again.
        boolean res2 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam2"))
                .qualifier(Bytes.toBytes("qual3")).ifEquals(null).thenDelete(delete1);
        // Print out the result, should be "Delete successful: true" since the checked column now is gone.
        System.out.println("Delete 2 successful: " + res2);

        Delete delete3 = new Delete(Bytes.toBytes("row2"));
        // Create yet another Delete instance, but using a different row.
        delete3.addFamily(Bytes.toBytes("colfam1"));

        try {
            // co CheckAndDeleteExample-8-CAS4 Try to delete while checking a different row.
            boolean res4 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"))
                    .qualifier(Bytes.toBytes("qual1")).ifEquals(Bytes.toBytes("val1")).thenDelete(delete3);
            // We will not get here as an exception is thrown beforehand!
            System.out.println("Delete 3 successful: " + res4);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        table.close();
        System.out.println("After delete call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);
        HBaseUtils.closeConnection();
    }
}
