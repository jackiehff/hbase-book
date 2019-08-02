package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ScanConsistencyExample1 Checks the scans behavior during concurrent modifications
 */
public class ScanConsistencyExample1 {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 5, 1, "colfam1");

        System.out.println("Table before the operations:");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE);

        Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);

        // vv ScanConsistencyExample1
        Scan scan = new Scan();
        // co ScanConsistencyExample1-1-ConfScan Configure scan to iterate over each row separately.
        scan.setCaching(1);
        ResultScanner scanner = table.getScanner(scan);

        // ^^ ScanConsistencyExample1
        System.out.println("Starting scan, reading one row...");
        // vv ScanConsistencyExample1
        Result result = scanner.next();
        HBaseUtils.dumpResult(result);

        // ^^ ScanConsistencyExample1
        System.out.println("Applying mutations...");
        // vv ScanConsistencyExample1
        Put put = new Put(Bytes.toBytes("row-3"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"), Bytes.toBytes("val-999"));
        // co ScanConsistencyExample1-2-Put Update a later row with a new value.
        table.put(put);

        Delete delete = new Delete(Bytes.toBytes("row-4"));
        // co ScanConsistencyExample1-3-Delete Remove an entire row, that is located at the end of the scan.
        table.delete(delete);

        System.out.println("Resuming original scan...");
        for (Result result2 : scanner) {
            // co ScanConsistencyExample1-4-Scan Scan the rest of the table to see if the mutations are visible.
            HBaseUtils.dumpResult(result2);
        }
        scanner.close();

        System.out.println("Print table under new scanner...");
        // co ScanConsistencyExample1-5-Dump Print the entire table again, with a new scanner instance.
        HBaseUtils.dump(HBaseConstants.TEST_TABLE);
        table.close();
        HBaseUtils.closeConnection();
    }
}
