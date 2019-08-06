package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ScanExample Example using a scanner to access data in a table
 */
public class ScanExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        // Tip: Remove comment below to enable padding, adjust start and stop row, as well as columns below to match. See scan #5 comments.
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 100, 100, /* 3, false, */ "colfam1", "colfam2");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            System.out.println("Scanning table #1...");
            // co ScanExample-1-NewScan Create empty Scan instance.
            Scan scan1 = new Scan();
            // co ScanExample-2-GetScanner Get a scanner to iterate over the rows.
            ResultScanner scanner1 = table.getScanner(scan1);
            for (Result res : scanner1) {
                // co ScanExample-3-Dump Print row content.
                System.out.println(res);
            }
            // co ScanExample-4-Close Close scanner to free remote resources.
            scanner1.close();

            System.out.println("Scanning table #2...");
            Scan scan2 = new Scan();
            // co ScanExample-5-AddColFam Add one column family only, this will suppress the retrieval of "colfam2".
            scan2.addFamily(Bytes.toBytes("colfam1"));
            ResultScanner scanner2 = table.getScanner(scan2);
            for (Result res : scanner2) {
                System.out.println(res);
            }
            scanner2.close();

            System.out.println("Scanning table #3...");

            Scan scan3 = new Scan();
            // co ScanExample-6-Build Use fluent pattern to add specific details to the Scan.
            scan3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")).
                    addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("col-33")).
                    withStartRow(Bytes.toBytes("row-10")).
                    withStopRow(Bytes.toBytes("row-20"));
            ResultScanner scanner3 = table.getScanner(scan3);
            for (Result res : scanner3) {
                System.out.println(res);
            }
            scanner3.close();

            System.out.println("Scanning table #4...");
            Scan scan4 = new Scan();
            // co ScanExample-7-Build Only select one column.
            scan4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5"))
                    .withStartRow(Bytes.toBytes("row-10"))
                    .withStopRow(Bytes.toBytes("row-20"));
            ResultScanner scanner4 = table.getScanner(scan4);
            for (Result res : scanner4) {
                System.out.println(res);
            }
            scanner4.close();

            System.out.println("Scanning table #5...");
            Scan scan5 = new Scan();
            // When using padding above, use "col-005", and "row-020", "row-010".
            scan5.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5"))
                    .withStartRow(Bytes.toBytes("row-20"))
                    .withStopRow(Bytes.toBytes("row-10"))
                    .setReversed(true); // co ScanExample-8-Build One column scan that runs in reverse.
            ResultScanner scanner5 = table.getScanner(scan5);
            for (Result res : scanner5) {
                System.out.println(res);
            }
            scanner5.close();
        }

        HBaseUtils.closeConnection();
    }
}
