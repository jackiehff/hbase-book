package filters;


import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.Filter;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ColumnPaginationFilterExample Example paginating through columns in a row
 */
public class ColumnPaginationFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 30, 2, true, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Filter filter = new ColumnPaginationFilter(5, 15);

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("Results of scan:");
            for (Result result : scanner) {
                System.out.println(result);
            }
            scanner.close();
        }

        HBaseUtils.closeConnection();
    }
}
