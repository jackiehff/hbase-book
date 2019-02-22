package filters;

// cc InclusiveStopFilterExample Example using a filter to include a stop row

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class InclusiveStopFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 100, 1, "colfam1");

        Table table = helper.getTable("testtable");
        // vv InclusiveStopFilterExample
        Filter filter = new InclusiveStopFilter(Bytes.toBytes("row-5"));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("row-3"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ InclusiveStopFilterExample
        System.out.println("Results of scan:");
        // vv InclusiveStopFilterExample
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
        // ^^ InclusiveStopFilterExample
    }
}
