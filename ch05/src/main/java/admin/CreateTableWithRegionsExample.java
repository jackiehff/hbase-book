package admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import util.HBaseUtils;

import java.io.IOException;

/**
 * CreateTableWithRegionsExample Example using the administrative API to create a table with predefined regions
 */
public class CreateTableWithRegionsExample {

    private static Connection connection = HBaseUtils.getConnection();

    private static void printTableRegions(String tableName) throws IOException {
        // co CreateTableWithRegionsExample-1-PrintTable HBaseUtils method to print the regions of a table.
        System.out.println("Printing regions of table: " + tableName);
        TableName tn = TableName.valueOf(tableName);
        RegionLocator locator = connection.getRegionLocator(tn);
        // co CreateTableWithRegionsExample-2-GetKeys Retrieve the start and end keys from the newly created table.
        Pair<byte[][], byte[][]> pair = locator.getStartEndKeys();
        for (int n = 0; n < pair.getFirst().length; n++) {
            byte[] sk = pair.getFirst()[n];
            byte[] ek = pair.getSecond()[n];
            // co CreateTableWithRegionsExample-3-Print Print the key, but guarding against the empty start (and end) key.
            System.out.println("[" + (n + 1) + "]" +
                    " start key: " +
                    (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) +
                    ", end key: " +
                    (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
        }
        locator.close();
    }

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable("testtable1");
        HBaseUtils.dropTable("testtable2");

        Admin admin = connection.getAdmin();

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("testtable1"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .build();

        // co CreateTableWithRegionsExample-4-CreateTable1 Call the createTable() method while also specifying the region boundaries.
        admin.createTable(tableDescriptor, Bytes.toBytes(1L), Bytes.toBytes(100L), 10);
        printTableRegions("testtable1");

        // co CreateTableWithRegionsExample-5-Regions Manually create region split keys.
        byte[][] regions = new byte[][]{
                Bytes.toBytes("A"),
                Bytes.toBytes("D"),
                Bytes.toBytes("G"),
                Bytes.toBytes("K"),
                Bytes.toBytes("O"),
                Bytes.toBytes("T")
        };

        TableDescriptor tableDescriptor2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("testtable2"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .build();
        // Call the createTable() method again, with a new table name and the list of region split keys.
        admin.createTable(tableDescriptor2, regions);
        printTableRegions("testtable2");
    }
}
