package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * InclusiveStopFilterExample Example using a filter to include a stop row
 */
public class InclusiveStopFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 100, 1, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
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
        }
        HBaseUtils.closeConnection();
    }
}
