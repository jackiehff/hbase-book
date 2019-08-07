package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import util.HBaseUtils;

import java.io.IOException;

/**
 * MissingRegionExample Example of how missing regions are handled
 */
public class MissingRegionExample {

    private static void printTableRegions() throws IOException {
        System.out.println("Printing regions of table: " + HBaseConstants.TEST_TABLE_NAME);
        RegionLocator locator = HBaseUtils.getConnection().getRegionLocator(HBaseConstants.TEST_TABLE);
        Pair<byte[][], byte[][]> pair = locator.getStartEndKeys();
        for (int n = 0; n < pair.getFirst().length; n++) {
            byte[] sk = pair.getFirst()[n];
            byte[] ek = pair.getSecond()[n];
            System.out.println("[" + (n + 1) + "]" +
                    " start key: " +
                    (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) +
                    ", end key: " +
                    (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
        }
    }

    // vv MissingRegionExample
    static class Getter implements Runnable { // co MissingRegionExample-1-Thread Use asynchronous thread to continuously read from the table.
        @Override
        public void run() {
            try {
                while (true) {
                    Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);
                    Get get = new Get(Bytes.toBytes("row-050"));
                    long time = System.currentTimeMillis();
                    table.get(get);
                    long diff = System.currentTimeMillis() - time;
                    if (diff > 1000) {
                        System.out.println("Wait time: " + diff + "ms"); // co MissingRegionExample-2-Print Print out waiting time if the get call was taking longer than a second to complete.
                    } else {
                        System.out.print(".");
                    }
                    try {
                        Thread.sleep(500); // co MissingRegionExample-3-Sleep1 Sleep for half a second.
                    } catch (InterruptedException e) {
                    }
                }
            } catch (IOException e) {
                System.err.println("Thread error: " + e);
            }
        }
    }

    // ^^ MissingRegionExample

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        byte[][] regions = new byte[][]{
                Bytes.toBytes("row-030"),
                Bytes.toBytes("row-060")
        };
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE_NAME, regions, "colfam1", "colfam2");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 100, 1, 3, false, "colfam1", "colfam2");
        printTableRegions();

        try (Admin admin = HBaseUtils.getConnection().getAdmin()) {
            Thread thread = new Thread(new Getter()); // co MissingRegionExample-4-Start Start the asynchronous thread.
            thread.setDaemon(true);
            thread.start();

            try {
                System.out.println("\nSleeping 3secs in main()..."); // co MissingRegionExample-5-Sleep2 Sleep for some time allow for the reading thread to print out some dots.
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                // ignore
            }

            RegionLocator locator = HBaseUtils.getConnection().getRegionLocator(HBaseConstants.TEST_TABLE);
            HRegionLocation location = locator.getRegionLocation(Bytes.toBytes("row-050"));
            System.out.println("\nUnassigning region: " + location.getRegion().getRegionNameAsString());
            admin.unassign(location.getRegion().getRegionName(), false); // co MissingRegionExample-6-Close Close the region containing the row the reading thread is retrieving. Note that unassign() does not work here because the master would automatically reopen the region when the thread is calling the get() method.

            int count = 0;
            while (locator.getAllRegionLocations().size() >= 3 && count++ < 10) { // co MissingRegionExample-7-Check Use the number of online regions to confirm the close.
                try {
                    System.out.println("\nWaiting for region to be offline in main()...");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }
            try {
                System.out.println("\nSleeping 10secs in main()...");
                Thread.sleep(10000); // co MissingRegionExample-8-Sleep3 Sleep for another period of time to make the thread wait.
            } catch (InterruptedException e) {
                // ignore
            }

            System.out.println("\nAssigning region: " + location.getRegion().getRegionNameAsString());
            admin.assign(location.getRegion().getRegionName()); // co MissingRegionExample-9-Open Open the region, which will make the blocked get() in the thread wake up and print its waiting time.

            try {
                System.out.println("\nSleeping another 3secs in main()...");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                // ignore
            }
            locator.close();
        }

        HBaseUtils.closeConnection();
    }
}
