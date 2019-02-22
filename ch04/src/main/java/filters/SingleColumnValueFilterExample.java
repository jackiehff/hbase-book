package filters;

// cc SingleColumnValueFilterExample Example using a filter to return only rows with a given value in a given column

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class SingleColumnValueFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

        Table table = helper.getTable("testtable");
        // vv SingleColumnValueFilterExample
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("colfam1"),
                Bytes.toBytes("col-5"),
                CompareOperator.NOT_EQUAL,
                new SubstringComparator("val-5"));
        filter.setFilterIfMissing(true);

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ SingleColumnValueFilterExample
        System.out.println("Results of scan:");
        // vv SingleColumnValueFilterExample
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }
        }
        scanner.close();

        Get get = new Get(Bytes.toBytes("row-6"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println("Result of get: ");
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength()));
        }

        table.close();
        helper.close();
    }
}
