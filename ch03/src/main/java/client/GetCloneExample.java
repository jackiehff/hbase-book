package client;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * GetCloneExample Example application retrieving data from HBase
 */
public class GetCloneExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        if (!helper.existsTable("testtable")) {
            helper.createTable("testtable", "colfam1");
        }
        Table table = helper.getTable("testtable");

        Get get1 = new Get(Bytes.toBytes("row1"));
        get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));

        Get get2 = new Get(get1);
        Result result = table.get(get2);
        System.out.println("Result : " + result);

        table.close();
        helper.close();
    }
}
