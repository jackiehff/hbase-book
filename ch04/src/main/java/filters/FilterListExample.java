package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FilterListExample Example of using a filter list to combine single purpose filters
 */
public class FilterListExample {

    public static void main(String[] args) throws IOException {

        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 5, 2, true, false, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            List<Filter> filters = new ArrayList<>();

            Filter filter1 = new RowFilter(CompareOperator.GREATER_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes("row-03")));
            filters.add(filter1);

            Filter filter2 = new RowFilter(CompareOperator.LESS_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes("row-06")));
            filters.add(filter2);

            Filter filter3 = new QualifierFilter(CompareOperator.EQUAL,
                    new RegexStringComparator("col-0[03]"));
            filters.add(filter3);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner scanner1 = table.getScanner(scan);
            // ^^ FilterListExample
            System.out.println("Results of scan #1 - MUST_PASS_ALL:");
            int n = 0;
            // vv FilterListExample
            for (Result result : scanner1) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                    // ^^ FilterListExample
                    n++;
                    // vv FilterListExample
                }
            }
            scanner1.close();

            FilterList filterList2 = new FilterList(
                    FilterList.Operator.MUST_PASS_ONE, filters);

            scan.setFilter(filterList2);
            ResultScanner scanner2 = table.getScanner(scan);
            // ^^ FilterListExample
            System.out.println("Total cell count for scan #1: " + n);
            n = 0;
            System.out.println("Results of scan #2 - MUST_PASS_ONE:");
            // vv FilterListExample
            for (Result result : scanner2) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                    // ^^ FilterListExample
                    n++;
                    // vv FilterListExample
                }
            }
            scanner2.close();
            // ^^ FilterListExample
            System.out.println("Total cell count for scan #2: " + n);
        }

        HBaseUtils.closeConnection();
    }
}
