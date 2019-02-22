package filters;

// cc RandomRowFilterExample Example filtering rows randomly

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class RandomRowFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 30, 0, true, "colfam1");

        Table table = helper.getTable("testtable");
        // vv RandomRowFilterExample
        Filter filter = new RandomRowFilter(0.5f);

        for (int loop = 1; loop <= 3; loop++) {
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            // ^^ RandomRowFilterExample
            System.out.println("Results of scan for loop: " + loop);
            // vv RandomRowFilterExample
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
            }
            scanner.close();
        }

        table.close();
        helper.close();
    }
}
