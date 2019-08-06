package coprocessor;

import constant.HBaseConstants;
import coprocessor.generated.RowCounterProtos;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.Map;

//import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

/**
 * EndpointExample Example using the custom row-count endpoint
 */
public class EndpointExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        HBaseUtils.put(HBaseConstants.TEST_TABLE,
                new String[]{"row1", "row2", "row3", "row4", "row5"},
                new String[]{"colfam1", "colfam2"},
                new String[]{"qual1", "qual1"},
                new long[]{1, 2},
                new String[]{"val1", "val2"});
        System.out.println("Before endpoint call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE,
                new String[]{"row1", "row2", "row3", "row4", "row5"},
                null, null);
        Admin admin = HBaseUtils.getConnection().getAdmin();
        try {
            admin.split(HBaseConstants.TEST_TABLE, Bytes.toBytes("row3"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // wait for the split to be done
        while (admin.getRegions(HBaseConstants.TEST_TABLE).size() < 2) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        //vv EndpointExample
        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            final RowCounterProtos.CountRequest request =
                    RowCounterProtos.CountRequest.getDefaultInstance();
            // co EndpointExample-3-Batch Create an anonymous class to be sent to all region servers.
            Map<byte[], Long> results = table.coprocessorService(
                    RowCounterProtos.RowCountService.class, // co EndpointExample-1-ClassName Define the protocol interface being invoked.
                    null, null, // co EndpointExample-2-Rows Set start and end row key to "null" to count all rows.
                    counter -> {
                        CoprocessorRpcUtils.BlockingRpcCallback<RowCounterProtos.CountResponse> rpcCallback =
                                new CoprocessorRpcUtils.BlockingRpcCallback<>();
                        // co EndpointExample-4-Call The call() method is executing the endpoint functions.
                        counter.getRowCount(null, request, rpcCallback);
                        RowCounterProtos.CountResponse response = rpcCallback.get();
                        return response.hasCount() ? response.getCount() : 0;
                    }
            );

            long total = 0;
            // co EndpointExample-5-Print Iterate over the returned map, containing the result for each region separately.
            for (Map.Entry<byte[], Long> entry : results.entrySet()) {
                total += entry.getValue();
                System.out.println("Region: " + Bytes.toString(entry.getKey()) +
                        ", Count: " + entry.getValue());
            }
            System.out.println("Total Count: " + total);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        HBaseUtils.closeConnection();
    }
}
