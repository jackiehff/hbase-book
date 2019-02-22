package client;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * ScanConsistencyExample2 Checks the scans behavior during concurrent modifications
 */
public class ScanConsistencyExample2 {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 5, 2, "colfam1");

        System.out.println("Table before the operations:");
        helper.dump("testtable");

        TableName tableName = TableName.valueOf("testtable");
        Table table = helper.getTable(tableName);

        Scan scan = new Scan();
        scan.setCaching(1);
        ResultScanner scanner = table.getScanner(scan);

        System.out.println("Starting scan, reading one row...");
        Result result = scanner.next();
        helper.dumpResult(result);

        System.out.println("Applying mutations...");
        Put put = new Put(Bytes.toBytes("row-3"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"),
                Bytes.toBytes("val-999"));
        table.put(put);

        Delete delete = new Delete(Bytes.toBytes("row-4"));
        table.delete(delete);

        System.out.println("Flushing and splitting table...");
        // vv ScanConsistencyExample2
        Admin admin = helper.getConnection().getAdmin();
        // co ScanConsistencyExample2-1-Flush Flush table and wait a little while for the operation to complete.
        admin.flush(tableName);
        try {
            Thread.currentThread().sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // co ScanConsistencyExample2-2-Split Split the table and wait until split operation has completed.
        admin.split(tableName, Bytes.toBytes("row-3"));
        while (admin.getRegions(tableName).size() == 1) {
        }

        // ^^ ScanConsistencyExample2
        System.out.println("Resuming original scan...");
        // vv ScanConsistencyExample2
        for (Result result2 : scanner) {
            helper.dumpResult(result2);
        }
        scanner.close();

        // ^^ ScanConsistencyExample2
        System.out.println("Print table under new scanner...");
        helper.dump("testtable");
        table.close();
        helper.close();
    }
}
