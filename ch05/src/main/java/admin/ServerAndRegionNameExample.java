package admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * ServerAndRegionNameExample Shows the use of server and region names
 */
public class ServerAndRegionNameExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

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

        RegionLocator locator = connection.getRegionLocator(tableName);
        HRegionLocation location = locator.getRegionLocation(Bytes.toBytes("Foo"));
        RegionInfo info = location.getRegion();
        System.out.println("Region Name: " + info.getRegionNameAsString());
        System.out.println("Server Name: " + location.getServerName());
        // ^^ ServerAndRegionNameExample
        locator.close();
        admin.close();
        connection.close();
    }
}
