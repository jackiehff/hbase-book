package coprocessor;

import coprocessor.generated.RowCounterProtos.CountRequest;
import coprocessor.generated.RowCounterProtos.CountResponse;
import coprocessor.generated.RowCounterProtos.RowCountService;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.Map;

/**
 * EndpointBatchExample Example using the custom row-count endpoint in batch mode
 */
public class EndpointBatchExample {

    public static void main(String[] args) throws IOException {
        TableName tableName = TableName.valueOf("testtable");
        HBaseUtils.dropTable(tableName);
        HBaseUtils.createTable(tableName, "colfam1", "colfam2");
        HBaseUtils.put(tableName,
                new String[]{"row1", "row2", "row3", "row4", "row5"},
                new String[]{"colfam1", "colfam2"},
                new String[]{"qual1", "qual1"},
                new long[]{1, 2},
                new String[]{"val1", "val2"});
        System.out.println("Before endpoint call...");
        HBaseUtils.dump(tableName,
                new String[]{"row1", "row2", "row3", "row4", "row5"},
                null, null);
        Admin admin = HBaseUtils.getConnection().getAdmin();
        try {
            admin.split(tableName, Bytes.toBytes("row3"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // wait for the split to be done
        while (admin.getRegions(tableName).size() < 2) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        Table table = HBaseUtils.getTable(tableName);
        try {
            final CountRequest request = CountRequest.getDefaultInstance();
            Map<byte[], CountResponse> results = table.batchCoprocessorService(
                    RowCountService.getDescriptor().findMethodByName("getRowCount"),
                    request, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
                    CountResponse.getDefaultInstance());

            long total = 0;
            for (Map.Entry<byte[], CountResponse> entry : results.entrySet()) {
                CountResponse response = entry.getValue();
                total += response.hasCount() ? response.getCount() : 0;
                System.out.println("Region: " + Bytes.toString(entry.getKey()) + ", Count: " + entry.getValue());
            }
            System.out.println("Total Count: " + total);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        admin.close();
        HBaseUtils.closeConnection();
    }
}
