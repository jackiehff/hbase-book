package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * DependentColumnFilterExample Example using a filter to include only specific column families
 */
public class DependentColumnFilterExample {

    private static Table table = null;

    private static void filter(boolean drop, CompareOperator operator, ByteArrayComparable comparator)
            throws IOException {
        Filter filter;
        if (comparator != null) {
            filter = new DependentColumnFilter(Bytes.toBytes("colfam1"), // co DependentColumnFilterExample-1-CreateFilter Create the filter with various options.
                    Bytes.toBytes("col-5"), drop, operator, comparator);
        } else {
            filter = new DependentColumnFilter(Bytes.toBytes("colfam1"),
                    Bytes.toBytes("col-5"), drop);
        }

        Scan scan = new Scan();
        scan.setFilter(filter);
        // scan.setBatch(4); // cause an error
        try(ResultScanner scanner = table.getScanner(scan)) {
            System.out.println("Results of scan:");
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                }
            }
        }

        Get get = new Get(Bytes.toBytes("row-5"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println("Result of get: ");
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength()));
        }
        System.out.println("");
    }

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 10, true, "colfam1", "colfam2");

        table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE);
        filter(true, CompareOperator.NO_OP, null);
        filter(false, CompareOperator.NO_OP, null); // co DependentColumnFilterExample-2-Filter Call filter method with various options.
        filter(true, CompareOperator.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes("val-5")));
        filter(false, CompareOperator.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes("val-5")));
        filter(true, CompareOperator.EQUAL,
                new RegexStringComparator(".*\\.5"));
        filter(false, CompareOperator.EQUAL,
                new RegexStringComparator(".*\\.5"));

        table.close();

        HBaseUtils.closeConnection();
    }
}
