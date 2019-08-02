package filters;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ColumnRangeFilterExample Example filtering by columns within a given range
 */
public class ColumnRangeFilterExample {

    public static void main(String[] args) throws IOException {

        HBaseUtils.dropTable("testtable");
        HBaseUtils.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable("testtable", 1, 10, 30, 2, true, "colfam1");

        Table table = HBaseUtils.getTable("testtable");
        // vv ColumnRangeFilterExample
        Filter filter = new ColumnRangeFilter(Bytes.toBytes("col-05"), true,
                Bytes.toBytes("col-11"), false);

        Scan scan = new Scan()
                .withStartRow(Bytes.toBytes("row-03"))
                .withStopRow(Bytes.toBytes("row-05"))
                .setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ ColumnRangeFilterExample
        System.out.println("Results of scan:");
        // vv ColumnRangeFilterExample
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
        // ^^ ColumnRangeFilterExample

        table.close();
        HBaseUtils.closeConnection();
    }
}
