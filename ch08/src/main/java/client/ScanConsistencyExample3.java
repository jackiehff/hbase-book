package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ScanConsistencyExample3 Checks the scans behavior across regions and concurrent changes
 */
public class ScanConsistencyExample3 {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        // vv ScanConsistencyExample3
        /*[*/
        byte[][] regions = new byte[][]{Bytes.toBytes("row-5")};
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE_NAME, regions, "colfam1");/*]*/

        // ^^ ScanConsistencyExample3
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 9, 1, "colfam1");

        System.out.println("Table before the operations:");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE);

        Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);

        // vv ScanConsistencyExample3
        Scan scan = new Scan();
        scan.setCaching(1);
        ResultScanner scanner = table.getScanner(scan);

        // ^^ ScanConsistencyExample3
        System.out.println("Starting scan, reading one row...");
        // vv ScanConsistencyExample3
        Result result = scanner.next();
        HBaseUtils.dumpResult(result);

        // ^^ ScanConsistencyExample3
        System.out.println("Applying mutations...");
        // vv ScanConsistencyExample3
        Put put = new Put(Bytes.toBytes("row-7"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"), Bytes.toBytes("val-999"));
        table.put(put);

        Delete delete = new Delete(Bytes.toBytes("row-8"));
        table.delete(delete);

        // ^^ ScanConsistencyExample3
        System.out.println("Resuming original scan...");
        // vv ScanConsistencyExample3
        for (Result result2 : scanner) {
            HBaseUtils.dumpResult(result2);
        }
        scanner.close();

        table.close();
        HBaseUtils.closeConnection();
    }
}
