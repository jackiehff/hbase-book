package coprocessor;

import constant.HBaseConstants;
import coprocessor.generated.RowCounterProtos;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import util.HBaseUtils;

import java.io.IOException;
import java.util.Map;


/**
 * EndpointCombinedExample Example extending the batch call to execute multiple endpoint calls
 */
public class EndpointCombinedExample {

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

        Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);
        // wait for the split to be done
        RegionLocator locator = HBaseUtils.getConnection().getRegionLocator(HBaseConstants.TEST_TABLE);
        while (locator.getAllRegionLocations().size() < 2) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        try {
            final RowCounterProtos.CountRequest request =
                    RowCounterProtos.CountRequest.getDefaultInstance();
            Map<byte[], Pair<Long, Long>> results = table.coprocessorService(
                    RowCounterProtos.RowCountService.class,
                    null, null,
                    counter -> {
                        CoprocessorRpcUtils.BlockingRpcCallback<RowCounterProtos.CountResponse> rowCallback =
                                new CoprocessorRpcUtils.BlockingRpcCallback<>();
                        counter.getRowCount(null, request, rowCallback);

                        CoprocessorRpcUtils.BlockingRpcCallback<RowCounterProtos.CountResponse> cellCallback =
                                new CoprocessorRpcUtils.BlockingRpcCallback<>();
                        counter.getCellCount(null, request, cellCallback);

                        RowCounterProtos.CountResponse rowResponse = rowCallback.get();
                        Long rowCount = rowResponse.hasCount() ?
                                rowResponse.getCount() : 0;

                        RowCounterProtos.CountResponse cellResponse = cellCallback.get();
                        Long cellCount = cellResponse.hasCount() ?
                                cellResponse.getCount() : 0;

                        return new Pair<>(rowCount, cellCount);
                    }
            );

            long totalRows = 0;
            long totalKeyValues = 0;
            for (Map.Entry<byte[], Pair<Long, Long>> entry : results.entrySet()) {
                totalRows += entry.getValue().getFirst();
                totalKeyValues += entry.getValue().getSecond();
                System.out.println("Region: " + Bytes.toString(entry.getKey()) + ", Count: " + entry.getValue());
            }
            System.out.println("Total Row Count: " + totalRows);
            System.out.println("Total Cell Count: " + totalKeyValues);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        admin.close();
        HBaseUtils.closeConnection();
    }
}
