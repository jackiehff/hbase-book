package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ScanConsistencyExample2 Checks the scans behavior during concurrent modifications
 */
public class ScanConsistencyExample2 {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 5, 2, "colfam1");

        System.out.println("Table before the operations:");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE);

        Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);

        Scan scan = new Scan();
        scan.setCaching(1);
        ResultScanner scanner = table.getScanner(scan);

        System.out.println("Starting scan, reading one row...");
        Result result = scanner.next();
        HBaseUtils.dumpResult(result);

        System.out.println("Applying mutations...");
        Put put = new Put(Bytes.toBytes("row-3"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"),
                Bytes.toBytes("val-999"));
        table.put(put);

        Delete delete = new Delete(Bytes.toBytes("row-4"));
        table.delete(delete);

        System.out.println("Flushing and splitting table...");
        // vv ScanConsistencyExample2
        Admin admin = HBaseUtils.getConnection().getAdmin();
        // co ScanConsistencyExample2-1-Flush Flush table and wait a little while for the operation to complete.
        admin.flush(HBaseConstants.TEST_TABLE);
        try {
            Thread.currentThread().sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // co ScanConsistencyExample2-2-Split Split the table and wait until split operation has completed.
        admin.split(HBaseConstants.TEST_TABLE, Bytes.toBytes("row-3"));
        while (admin.getRegions(HBaseConstants.TEST_TABLE).size() == 1) {
        }

        // ^^ ScanConsistencyExample2
        System.out.println("Resuming original scan...");
        // vv ScanConsistencyExample2
        for (Result result2 : scanner) {
            HBaseUtils.dumpResult(result2);
        }
        scanner.close();

        // ^^ ScanConsistencyExample2
        System.out.println("Print table under new scanner...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE);
        table.close();
        HBaseUtils.closeConnection();
    }
}
