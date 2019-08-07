package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * RowFilterExample Example using a filter to select specific rows
 */
public class RowFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 100, 100, "colfam1", "colfam2");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"));

            Filter filter1 = new RowFilter(CompareOperator.LESS_OR_EQUAL, // co RowFilterExample-1-Filter1 Create filter, while specifying the comparison operator and comparator. Here an exact match is needed.
                    new BinaryComparator(Bytes.toBytes("row-22")));
            scan.setFilter(filter1);
            try(ResultScanner scanner1 = table.getScanner(scan)) {
                System.out.println("Scanning table #1...");
                for (Result res : scanner1) {
                    System.out.println(res);
                }
            }

            Filter filter2 = new RowFilter(CompareOperator.EQUAL, // co RowFilterExample-2-Filter2 Another filter, this time using a regular expression to match the row keys.
                    new RegexStringComparator(".*-.5"));
            scan.setFilter(filter2);
            try(ResultScanner scanner2 = table.getScanner(scan)) {
                System.out.println("Scanning table #2...");
                for (Result res : scanner2) {
                    System.out.println(res);
                }
            }

            // co RowFilterExample-3-Filter3 The third filter uses a substring match approach.
            Filter filter3 = new RowFilter(CompareOperator.EQUAL,
                    new SubstringComparator("-5"));
            scan.setFilter(filter3);
            try(ResultScanner scanner3 = table.getScanner(scan)) {
                System.out.println("Scanning table #3...");
                for (Result res : scanner3) {
                    System.out.println(res);
                }
            }
        }
        HBaseUtils.closeConnection();
    }
}
