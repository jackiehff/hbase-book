package filters;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ColumnPrefixFilterExample Example filtering by column prefix
 */
public class ColumnPrefixFilterExample {

    public static void main(String[] args) throws IOException {

        HBaseUtils.dropTable("testtable");
        HBaseUtils.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable("testtable", 1, 10, 30, 0, true, "colfam1");

        Table table = HBaseUtils.getTable("testtable");
        // vv ColumnPrefixFilterExample
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("col-1"));

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ ColumnPrefixFilterExample
        System.out.println("Results of scan:");
        // vv ColumnPrefixFilterExample
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
        // ^^ ColumnPrefixFilterExample

        table.close();
        HBaseUtils.closeConnection();
    }
}
