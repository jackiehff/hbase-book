package client;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import util.HBaseHelper;

import java.io.IOException;

/**
 * ScanTimeoutExample Example timeout while using a scanner
 */
public class ScanTimeoutExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

        Table table = helper.getTable("testtable");

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        // co ScanTimeoutExample-1-GetConf Get currently configured lease timeout.
        int scannerTimeout = (int) helper.getConfiguration().getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1);
        System.out.println("Current (local) lease period: " + scannerTimeout + "ms");
        System.out.println("Sleeping now for " + (scannerTimeout + 5000) + "ms...");
        try {
            // co ScanTimeoutExample-2-Sleep Sleep a little longer than the lease allows.
            Thread.sleep(scannerTimeout + 5000);
        } catch (InterruptedException e) {
            // ignore
        }
        System.out.println("Attempting to iterate over scanner...");
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
        scanner.close();
        table.close();
        helper.close();
    }
}
