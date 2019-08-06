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
 * ScanSlicingExample Example using offset and limit parameters for scans
 */
public class ScanSlicingExample {

    private static Table table = null;

    private static void scan(int num, int caching, int batch, int offset,
                             int maxResults, int maxResultSize, boolean dump) throws IOException {
        int count = 0;
        Scan scan = new Scan()
                .setCaching(caching)
                .setBatch(batch)
                .setRowOffsetPerColumnFamily(offset)
                .setMaxResultsPerColumnFamily(maxResults)
                .setMaxResultSize(maxResultSize)
                .setScanMetricsEnabled(true);
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("Scan #" + num + " running...");
        for (Result result : scanner) {
            count++;
            if (dump) {
                System.out.println("Result [" + count + "]:" + result);
            }
        }
        ScanMetrics metrics = scanner.getScanMetrics();
        System.out.println("Caching: " + caching + ", Batch: " + batch +
                ", Offset: " + offset + ", maxResults: " + maxResults +
                ", maxSize: " + maxResultSize + ", Results: " + count +
                ", RPCs: " + metrics.countOfRPCcalls);
        scanner.close();
    }

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 10, 2, true, "colfam1", "colfam2");

        table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);

        scan(1, 11, 0, 0, 2, -1, true);
        scan(2, 11, 0, 4, 2, -1, true);
        scan(3, 5, 0, 0, 2, -1, false);
        scan(4, 11, 2, 0, 5, -1, true);
        scan(5, 11, -1, -1, -1, 1, false);
        scan(6, 11, -1, -1, -1, 10000, false);

        table.close();

        HBaseUtils.closeConnection();
    }
}
