package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * QualifierFilterExample Example using a filter to include only specific column qualifiers
 */
public class QualifierFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 10, "colfam1", "colfam2");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Filter filter = new QualifierFilter(CompareOperator.LESS_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes("col-2")));

            Scan scan = new Scan();
            scan.setFilter(filter);
            try (ResultScanner scanner = table.getScanner(scan)) {
                System.out.println("Scanning table... ");
                for (Result result : scanner) {
                    System.out.println(result);
                }
            }

            Get get = new Get(Bytes.toBytes("row-5"));
            get.setFilter(filter);
            Result result = table.get(get);
            System.out.println("Result of get(): " + result);
        }

        HBaseUtils.closeConnection();
    }
}
