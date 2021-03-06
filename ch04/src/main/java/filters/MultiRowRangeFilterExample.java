package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;

/**
 * MultiRowRangeFilterExample Example using the multi-row-range filter
 */
public class MultiRowRangeFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 100, 10, 3, false, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            List<RowRange> ranges = new ArrayList<>();
            ranges.add(new RowRange(Bytes.toBytes("row-010"), true,
                    Bytes.toBytes("row-020"), false));
            ranges.add(new RowRange(Bytes.toBytes("row-050"), true,
                    Bytes.toBytes("row-090"), true));
            ranges.add(new RowRange(Bytes.toBytes("row-096"), true,
                    Bytes.toBytes("row-097"), false));

            Filter filter = new MultiRowRangeFilter(ranges);

            Scan scan = new Scan().withStartRow(Bytes.toBytes("row-005")).withStopRow(Bytes.toBytes("row-110"));
            scan.setFilter(filter);

            try (ResultScanner scanner = table.getScanner(scan)) {
                System.out.println("Results of scan:");
                int numRows = 0;
                for (Result result : scanner) {
                    for (Cell cell : result.rawCells()) {
                        System.out.println("Cell: " + cell + ", Value: " +
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                        cell.getValueLength()));
                    }
                    numRows++;
                }
                System.out.println("Number of rows: " + numRows);
            }
        }

        HBaseUtils.closeConnection();
    }
}
