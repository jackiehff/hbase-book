package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * BatchCallbackExample Example application using batch operations with callbacks
 */
public class BatchCallbackExample {

    private final static byte[] ROW1 = Bytes.toBytes("row1");
    private final static byte[] ROW2 = Bytes.toBytes("row2");
    private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
    private final static byte[] COLFAM2 = Bytes.toBytes("colfam2");
    private final static byte[] QUAL1 = Bytes.toBytes("qual1");
    private final static byte[] QUAL2 = Bytes.toBytes("qual2");

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        HBaseUtils.put(HBaseConstants.TEST_TABLE,
                new String[]{"row1"},
                new String[]{"colfam1"},
                new String[]{"qual1", "qual2", "qual3"},
                new long[]{1, 2, 3},
                new String[]{"val1", "val2", "val3"});
        System.out.println("Before batch call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1", "row2"}, null, null);

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            // co BatchCallbackExample-1-CreateList Create a list to hold all values.
            List<Row> batch = new ArrayList<>();

            Put put = new Put(ROW2);
            // co BatchCallbackExample-2-AddPut Add a Put instance.
            put.addColumn(COLFAM2, QUAL1, 4, Bytes.toBytes("val5"));
            batch.add(put);

            Get get1 = new Get(ROW1);
            // co BatchCallbackExample-3-AddGet Add a Get instance for a different row.
            get1.addColumn(COLFAM1, QUAL1);
            batch.add(get1);

            Delete delete = new Delete(ROW1);
            // co BatchCallbackExample-4-AddDelete Add a Delete instance.
            delete.addColumns(COLFAM1, QUAL2);
            batch.add(delete);

            Get get2 = new Get(ROW2);
            // co BatchCallbackExample-5-AddBogus Add a Get instance that will fail.
            get2.addFamily(Bytes.toBytes("BOGUS"));
            batch.add(get2);

            // co BatchCallbackExample-6-CreateResult Create result array.
            Object[] results = new Object[batch.size()];
            try {
                table.batchCallback(batch, results, (Batch.Callback<Result>) (region, row, result) -> System.out.println("Received callback for row[" +
                        Bytes.toString(row) + "] -> " + result));
            } catch (Exception e) {
                // co BatchCallbackExample-7-Print Print error that was caught.
                System.err.println("Error: " + e);
            }

            for (int i = 0; i < results.length; i++) {
                // co BatchCallbackExample-8-Dump Print all results and class types.
                System.out.println("Result[" + i + "]: type = " +
                        results[i].getClass().getSimpleName() + "; " + results[i]);
            }
        }
        System.out.println("After batch call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1", "row2"}, null, null);
        HBaseUtils.closeConnection();
    }
}
