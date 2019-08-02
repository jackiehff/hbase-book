package admin;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClusterOperationExample Shows the use of the cluster operations
 */
public class ClusterOperationExample {

    private static void printRegionInfo(List<RegionInfo> infos) {
        for (RegionInfo info : infos) {
            System.out.println("  Start Key: " + Bytes.toString(info.getStartKey()));
        }
    }

    private static List<RegionInfo> filterTableRegions(List<RegionInfo> regions,
                                                       TableName tableName) {
        List<RegionInfo> filtered = new ArrayList<>();
        for (RegionInfo info : regions) {
            if (info.getTable().equals(tableName)) {
                filtered.add(info);
            }
        }
        return filtered;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        // vv ClusterOperationExample
        Admin admin = HBaseUtils.getConnection().getAdmin();

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(HBaseConstants.TEST_TABLE)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .setValue("Description", "Chapter 5 - ClusterOperationExample")
                .build();
        byte[][] regions = new byte[][]{Bytes.toBytes("ABC"),
                Bytes.toBytes("DEF"), Bytes.toBytes("GHI"), Bytes.toBytes("KLM"),
                Bytes.toBytes("OPQ"), Bytes.toBytes("TUV")
        };

        // co ClusterOperationExample-01-Create Create a table with seven regions, and one column family.
        admin.createTable(tableDescriptor, regions);

        BufferedMutator mutator = HBaseUtils.getConnection().getBufferedMutator(HBaseConstants.TEST_TABLE);
        for (int a = 'A'; a <= 'Z'; a++) {
            for (int b = 'A'; b <= 'Z'; b++) {
                for (int c = 'A'; c <= 'Z'; c++) {
                    String row = Character.toString((char) a) +
                            (char) b + (char) c; // co ClusterOperationExample-02-Put Insert many rows starting from "AAA" to "ZZZ". These will be spread across the regions.
                    Put put = new Put(Bytes.toBytes(row));
                    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col1"),
                            Bytes.toBytes("val1"));
                    System.out.println("Adding row: " + row);
                    mutator.mutate(put);
                }
            }
        }
        mutator.close();

        List<RegionInfo> list = admin.getRegions(HBaseConstants.TEST_TABLE);
        int numRegions = list.size();
        RegionInfo info = list.get(numRegions - 1);
        System.out.println("Number of regions: " + numRegions); // co ClusterOperationExample-03-List List details about the regions.
        System.out.println("Regions: ");
        printRegionInfo(list);

        System.out.println("Splitting region: " + info.getRegionNameAsString());
        admin.splitRegionAsync(info.getRegionName()); // co ClusterOperationExample-04-Split Split the last region this table has, starting at row key "TUV". Adds a new region starting with key "WEI".
        do {
            list = admin.getRegions(HBaseConstants.TEST_TABLE);
            Thread.sleep(1 * 1000L);
            System.out.print(".");
        }
        while (list.size() <= numRegions); // co ClusterOperationExample-05-Wait Loop and check until the operation has taken effect.
        numRegions = list.size();
        System.out.println();
        System.out.println("Number of regions: " + numRegions);
        System.out.println("Regions: ");
        printRegionInfo(list);

        System.out.println("Retrieving region with row ZZZ...");
        RegionLocator locator = HBaseUtils.getConnection().getRegionLocator(HBaseConstants.TEST_TABLE);
        HRegionLocation location =
                locator.getRegionLocation(Bytes.toBytes("ZZZ")); // co ClusterOperationExample-06-Cache Retrieve region infos cached and refreshed to show the difference.
        System.out.println("Found cached region: " +
                location.getRegion().getRegionNameAsString());
        location = locator.getRegionLocation(Bytes.toBytes("ZZZ"), true);
        System.out.println("Found refreshed region: " +
                location.getRegion().getRegionNameAsString());

        List<RegionInfo> online = admin.getRegions(location.getServerName());
        online = filterTableRegions(online, HBaseConstants.TEST_TABLE);
        int numOnline = online.size();
        System.out.println("Number of online regions: " + numOnline);
        System.out.println("Online Regions: ");
        printRegionInfo(online);

        RegionInfo offline = online.get(online.size() - 1);
        System.out.println("Offlining region: " + offline.getRegionNameAsString());
        admin.offline(offline.getRegionName()); // co ClusterOperationExample-07-Offline Offline a region and print the list of all regions.
        int revs = 0;
        do {
            online = admin.getRegions(location.getServerName());
            online = filterTableRegions(online, HBaseConstants.TEST_TABLE);
            Thread.sleep(1 * 1000L);
            System.out.print(".");
            revs++;
        } while (online.size() <= numOnline && revs < 10);
        numOnline = online.size();
        System.out.println();
        System.out.println("Number of online regions: " + numOnline);
        System.out.println("Online Regions: ");
        printRegionInfo(online);

        RegionInfo split = online.get(0); // co ClusterOperationExample-08-Wrong Attempt to split a region with a split key that does not fall into boundaries. Triggers log message.
        System.out.println("Splitting region with wrong key: " +
                split.getRegionNameAsString());
        admin.splitRegionAsync(split.getRegionName(), Bytes.toBytes("ZZZ")); // triggers log message

        System.out.println("Assigning region: " + offline.getRegionNameAsString());
        // co ClusterOperationExample-09-Reassign Reassign the offlined region.
        admin.assign(offline.getRegionName());
        revs = 0;
        do {
            online = admin.getRegions(location.getServerName());
            online = filterTableRegions(online, HBaseConstants.TEST_TABLE);
            Thread.sleep(1 * 1000L);
            System.out.print(".");
            revs++;
        } while (online.size() == numOnline && revs < 10);
        numOnline = online.size();
        System.out.println();
        System.out.println("Number of online regions: " + numOnline);
        System.out.println("Online Regions: ");
        printRegionInfo(online);

        System.out.println("Merging regions...");
        RegionInfo m1 = online.get(0);
        RegionInfo m2 = online.get(1);
        System.out.println("Regions: " + m1 + " with " + m2);
        // co ClusterOperationExample-10-Merge Merge the first two regions. Print out result of operation.
        admin.mergeRegionsAsync(m1.getEncodedNameAsBytes(), m2.getEncodedNameAsBytes(), false);
        revs = 0;
        do {
            list = admin.getRegions(HBaseConstants.TEST_TABLE);
            Thread.sleep(1 * 1000L);
            System.out.print(".");
            revs++;
        } while (list.size() >= numRegions && revs < 10);
        numRegions = list.size();
        System.out.println();
        System.out.println("Number of regions: " + numRegions);
        System.out.println("Regions: ");
        printRegionInfo(list);

        // ^^ ClusterOperationExample
        locator.close();
        admin.close();
        HBaseUtils.closeConnection();
    }
}
