package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * BatchSameRowExample Example application using batch operations, modifying the same row
 */
public class BatchSameRowExample {

    private final static byte[] ROW1 = Bytes.toBytes("row1");
    private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
    private final static byte[] QUAL1 = Bytes.toBytes("qual1");

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        HBaseUtils.put(HBaseConstants.TEST_TABLE, "row1", "colfam1", "qual1", 1L, "val1");
        System.out.println("Before batch call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            List<Row> batch = new ArrayList<>();

            Put put = new Put(ROW1);
            put.addColumn(COLFAM1, QUAL1, 2L, Bytes.toBytes("val2"));
            batch.add(put);

            Get get1 = new Get(ROW1);
            get1.addColumn(COLFAM1, QUAL1);
            batch.add(get1);

            Delete delete = new Delete(ROW1);
            // co BatchSameRowExample-1-AddDelete Delete the row that was just put above.
            delete.addColumns(COLFAM1, QUAL1, 3L);
            batch.add(delete);

            Get get2 = new Get(ROW1);
            get1.addColumn(COLFAM1, QUAL1);
            batch.add(get2);

            Object[] results = new Object[batch.size()];
            try {
                table.batch(batch, results);
            } catch (Exception e) {
                System.err.println("Error: " + e);
            }

            for (int i = 0; i < results.length; i++) {
                System.out.println("Result[" + i + "]: type = " +
                        results[i].getClass().getSimpleName() + "; " + results[i]);
            }
        }

        System.out.println("After batch call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);
        HBaseUtils.closeConnection();
    }
}
