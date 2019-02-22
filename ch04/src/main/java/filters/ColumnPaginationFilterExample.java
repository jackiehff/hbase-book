package filters;

// cc ColumnPaginationFilterExample Example paginating through columns in a row

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.Filter;
import util.HBaseHelper;

import java.io.IOException;

public class ColumnPaginationFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 30, 2, true, "colfam1");

        Table table = helper.getTable("testtable");
        // vv ColumnPaginationFilterExample
        Filter filter = new ColumnPaginationFilter(5, 15);

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ ColumnPaginationFilterExample
        System.out.println("Results of scan:");
        // vv ColumnPaginationFilterExample
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
        // ^^ ColumnPaginationFilterExample

        table.close();
        helper.close();
    }
}
