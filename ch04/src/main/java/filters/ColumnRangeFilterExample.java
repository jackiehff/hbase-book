package filters;

// cc ColumnRangeFilterExample Example filtering by columns within a given range

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class ColumnRangeFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 30, 2, true, "colfam1");

        Table table = helper.getTable("testtable");
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
        helper.close();
    }
}
