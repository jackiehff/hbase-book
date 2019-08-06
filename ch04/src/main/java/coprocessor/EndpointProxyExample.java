package coprocessor;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

import static coprocessor.generated.RowCounterProtos.*;

/**
 * EndpointProxyExample Example using the proxy call of HTable to invoke an endpoint on a single region
 */
public class EndpointProxyExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, 3, "colfam1", "colfam2");
        HBaseUtils.put(HBaseConstants.TEST_TABLE,
                new String[]{"row1", "row2", "row3", "row4", "row5"},
                new String[]{"colfam1", "colfam2"}, new String[]{"qual1", "qual1"},
                new long[]{1, 2}, new String[]{"val1", "val2"});
        System.out.println("Before endpoint call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1", "row2", "row3", "row4", "row5"}, null, null);
        Admin admin = HBaseUtils.getConnection().getAdmin();
        try {
            admin.split(HBaseConstants.TEST_TABLE, Bytes.toBytes("row3"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        // wait for the split to be done
        while (admin.getRegions(HBaseConstants.TEST_TABLE).size() < 2) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            //vv EndpointProxyExample
            RegionInfo hri = admin.getRegions(HBaseConstants.TEST_TABLE).get(0);
            Scan scan = new Scan().withStartRow(hri.getStartKey()).withStopRow(hri.getEndKey()).setMaxVersions();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println("Result: " + result);
            }

            CoprocessorRpcChannel channel = table.coprocessorService(Bytes.toBytes("row1"));
            RowCountService.BlockingInterface service = RowCountService.newBlockingStub(channel);
            CountRequest request = CountRequest.newBuilder().build();
            CountResponse response = service.getCellCount(null, request);
            long cellsInRegion = response.hasCount() ? response.getCount() : -1;
            System.out.println("Region Cell Count: " + cellsInRegion);

            request = CountRequest.newBuilder().build();
            response = service.getRowCount(null, request);
            long rowsInRegion = response.hasCount() ? response.getCount() : -1;
            System.out.println("Region Row Count: " + rowsInRegion);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        admin.close();
        HBaseUtils.closeConnection();
    }
}
