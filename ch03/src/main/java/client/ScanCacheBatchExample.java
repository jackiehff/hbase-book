package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ScanCacheBatchExample Example using caching and batch parameters for scans
 */
public class ScanCacheBatchExample {

    private static Table table = null;

    private static void scan(int caching, int batch, boolean small)
            throws IOException {
        int count = 0;
        Scan scan = new Scan()
                .setCaching(caching)  // co ScanCacheBatchExample-1-Set Set caching and batch parameters.
                .setBatch(batch)
                .setSmall(small)
                .setScanMetricsEnabled(true);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            // co ScanCacheBatchExample-2-Count Count the number of Results available.
            count++;
        }
        ScanMetrics metrics = scanner.getScanMetrics();
        System.out.println("Caching: " + caching + ", Batch: " + batch +
                ", Small: " + small + ", Results: " + count +
                ", RPCs: " + metrics.countOfRPCcalls);

        scanner.close();
    }

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 10, "colfam1", "colfam2");

        table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);

        scan(1, 1, false);
        scan(1, 0, false);
        scan(1, 0, true);
        scan(200, 1, false);
        scan(200, 0, false);
        scan(200, 0, true);
        // co ScanCacheBatchExample-3-Test Test various combinations.
        scan(2000, 100, false);
        scan(2, 100, false);
        scan(2, 10, false);
        scan(5, 100, false);
        scan(5, 20, false);
        scan(10, 10, false);

        table.close();
        HBaseUtils.closeConnection();
    }
}
