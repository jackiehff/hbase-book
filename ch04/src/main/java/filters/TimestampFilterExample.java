package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * TimestampFilterExample Example filtering data by timestamps
 */
public class TimestampFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 100, 20, true, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            List<Long> ts = new ArrayList<>();
            ts.add(5L);
            ts.add(10L); // co TimestampFilterExample-1-AddTS Add timestamps to the list.
            ts.add(15L);
            Filter filter = new TimestampsFilter(ts);

            Scan scan1 = new Scan();
            scan1.setFilter(filter); // co TimestampFilterExample-2-AddFilter Add the filter to an otherwise default Scan instance.
            ResultScanner scanner1 = table.getScanner(scan1);
            // ^^ TimestampFilterExample
            System.out.println("Results of scan #1:");
            // vv TimestampFilterExample
            for (Result result : scanner1) {
                System.out.println(result);
            }
            scanner1.close();

            Scan scan2 = new Scan();
            scan2.setFilter(filter);
            scan2.setTimeRange(8, 12); // co TimestampFilterExample-3-AddTSRange Also add a time range to verify how it affects the filter
            ResultScanner scanner2 = table.getScanner(scan2);
            // ^^ TimestampFilterExample
            System.out.println("Results of scan #2:");
            // vv TimestampFilterExample
            for (Result result : scanner2) {
                System.out.println(result);
            }
            scanner2.close();
        }
        HBaseUtils.closeConnection();
    }
}
