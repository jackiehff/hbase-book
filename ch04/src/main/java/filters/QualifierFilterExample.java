package filters;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * QualifierFilterExample Example using a filter to include only specific column qualifiers
 */
public class QualifierFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

        Table table = helper.getTable("testtable");

        // vv QualifierFilterExample
        Filter filter = new QualifierFilter(CompareOperator.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("col-2")));

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ QualifierFilterExample
        System.out.println("Scanning table... ");
        // vv QualifierFilterExample
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();

        Get get = new Get(Bytes.toBytes("row-5"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println("Result of get(): " + result);

        table.close();
        helper.close();
    }
}
