package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * SkipFilterExample Example of using a filter to skip entire rows based on another filter's results
 */
public class SkipFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 30, 5, 2, true, true, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Filter filter1 = new ValueFilter(CompareOperator.NOT_EQUAL,
                    new BinaryComparator(Bytes.toBytes("val-0")));

            Scan scan = new Scan();
            // co SkipFilterExample-1-AddFilter1 Only add the ValueFilter to the first scan.
            scan.setFilter(filter1);
            ResultScanner scanner1 = table.getScanner(scan);
            // ^^ SkipFilterExample
            System.out.println("Results of scan #1:");
            int n = 0;
            // vv SkipFilterExample
            for (Result result : scanner1) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                    // ^^ SkipFilterExample
                    n++;
                    // vv SkipFilterExample
                }
            }
            scanner1.close();

            Filter filter2 = new SkipFilter(filter1);
            // co SkipFilterExample-2-AddFilter2 Add the decorating skip filter for the second scan.
            scan.setFilter(filter2);
            ResultScanner scanner2 = table.getScanner(scan);
            // ^^ SkipFilterExample
            System.out.println("Total cell count for scan #1: " + n);
            n = 0;
            System.out.println("Results of scan #2:");
            // vv SkipFilterExample
            for (Result result : scanner2) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                    // ^^ SkipFilterExample
                    n++;
                    // vv SkipFilterExample
                }
            }
            scanner2.close();
            // ^^ SkipFilterExample
            System.out.println("Total cell count for scan #2: " + n);
        }
        HBaseUtils.closeConnection();
    }
}
