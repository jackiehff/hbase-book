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
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * WhileMatchFilterExample Example of using a filter to skip entire rows based on another filter's results
 */
public class WhileMatchFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 1, 2, true, false, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Filter filter1 = new RowFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("row-05")));

            Scan scan = new Scan();
            scan.setFilter(filter1);
            try (ResultScanner scanner1 = table.getScanner(scan)) {
                System.out.println("Results of scan #1:");
                int n = 0;
                for (Result result : scanner1) {
                    for (Cell cell : result.rawCells()) {
                        System.out.println("Cell: " + cell + ", Value: " +
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                        cell.getValueLength()));
                        n++;
                    }
                }
                System.out.println("Total cell count for scan #1: " + n);
            }

            Filter filter2 = new WhileMatchFilter(filter1);
            scan.setFilter(filter2);
            try (ResultScanner scanner2 = table.getScanner(scan)) {

                int n = 0;
                System.out.println("Results of scan #2:");
                for (Result result : scanner2) {
                    for (Cell cell : result.rawCells()) {
                        System.out.println("Cell: " + cell + ", Value: " +
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                        cell.getValueLength()));
                        n++;
                    }
                }
                System.out.println("Total cell count for scan #2: " + n);
            }
        }

        HBaseUtils.closeConnection();
    }
}
