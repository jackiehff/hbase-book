package admin;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ServerAndRegionNameExample Shows the use of server and region names
 */
public class ServerAndRegionNameExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        Admin admin = HBaseUtils.getConnection().getAdmin();

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(HBaseConstants.TEST_TABLE)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .setValue("Description", "Chapter 5 - ServerAndRegionNameExample").build();

        byte[][] regions = new byte[][]{Bytes.toBytes("ABC"),
                Bytes.toBytes("DEF"), Bytes.toBytes("GHI"), Bytes.toBytes("KLM"),
                Bytes.toBytes("OPQ"), Bytes.toBytes("TUV")
        };
        admin.createTable(tableDescriptor, regions);

        RegionLocator locator = HBaseUtils.getConnection().getRegionLocator(HBaseConstants.TEST_TABLE);
        HRegionLocation location = locator.getRegionLocation(Bytes.toBytes("Foo"));
        RegionInfo info = location.getRegion();
        System.out.println("Region Name: " + info.getRegionNameAsString());
        System.out.println("Server Name: " + location.getServerName());

        locator.close();
        admin.close();
        HBaseUtils.closeConnection();
    }
}
