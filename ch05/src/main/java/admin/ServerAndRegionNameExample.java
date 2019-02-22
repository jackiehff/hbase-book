package admin;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * ServerAndRegionNameExample Shows the use of server and region names
 */
public class ServerAndRegionNameExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        Admin admin = helper.getConnection().getAdmin();

        // vv ServerAndRegionNameExample
        TableName tableName = TableName.valueOf("testtable");
        HColumnDescriptor coldef1 = new HColumnDescriptor("colfam1");
        HTableDescriptor desc = new HTableDescriptor(tableName)
                .addFamily(coldef1)
                .setValue("Description", "Chapter 5 - ServerAndRegionNameExample");
        byte[][] regions = new byte[][]{Bytes.toBytes("ABC"),
                Bytes.toBytes("DEF"), Bytes.toBytes("GHI"), Bytes.toBytes("KLM"),
                Bytes.toBytes("OPQ"), Bytes.toBytes("TUV")
        };
        admin.createTable(desc, regions);

        RegionLocator locator = helper.getConnection().getRegionLocator(tableName);
        HRegionLocation location = locator.getRegionLocation(Bytes.toBytes("Foo"));
        RegionInfo info = location.getRegion();
        System.out.println("Region Name: " + info.getRegionNameAsString());
        System.out.println("Server Name: " + location.getServerName());
        // ^^ ServerAndRegionNameExample
        locator.close();
        admin.close();
        helper.close();
    }
}
