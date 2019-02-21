package client;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * AppendExample Example application appending data to a column in  HBase
 */
public class AppendExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", 100, "colfam1", "colfam2");
        helper.put("testtable",
                new String[]{"row1"},
                new String[]{"colfam1"},
                new String[]{"qual1"},
                new long[]{1},
                new String[]{"oldvalue"});
        System.out.println("Before append call...");
        helper.dump("testtable", new String[]{"row1"}, null, null);

        Table table = helper.getTable("testtable");
        Append append = new Append(Bytes.toBytes("row1"));
        append.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("newvalue"));
        append.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("anothervalue"));
        table.append(append);

        System.out.println("After append call...");
        helper.dump("testtable", new String[]{"row1"}, null, null);
        table.close();
        helper.close();
    }
}
