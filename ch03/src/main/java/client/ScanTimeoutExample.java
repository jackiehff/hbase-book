package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ScanTimeoutExample Example timeout while using a scanner
 */
public class ScanTimeoutExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanTimeoutExample.class);

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 10, "colfam1", "colfam2");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            // co ScanTimeoutExample-1-GetConf Get currently configured lease timeout.
            int scannerTimeout = (int) HBaseUtils.getConfiguration().getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1);
            System.out.println("Current (local) lease period: " + scannerTimeout + "ms");
            System.out.println("Sleeping now for " + (scannerTimeout + 5000) + "ms...");
            try {
                // co ScanTimeoutExample-2-Sleep Sleep a little longer than the lease allows.
                Thread.sleep(scannerTimeout + 5000);
            } catch (InterruptedException e) {
                // ignore
                LOGGER.error("InterruptedException", e);
            }
            System.out.println("Attempting to iterate over scanner...");
            Scan scan = new Scan();
            try (ResultScanner scanner = table.getScanner(scan)) {
                while (true) {
                    try {
                        Result result = scanner.next();
                        if (result == null) {
                            break;
                        }
                        // co ScanTimeoutExample-3-Dump Print row content.
                        System.out.println(result);
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }
        }

        HBaseUtils.closeConnection();
    }
}
