package filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * CustomFilterExample Example using a custom filter
 */
public class CustomFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 10, 2, true, "colfam1");

        Table table = helper.getTable("testtable");
        // vv CustomFilterExample
        List<Filter> filters = new ArrayList<>();

        Filter filter1 = new CustomFilter(Bytes.toBytes("val-05.05"));
        filters.add(filter1);

        Filter filter2 = new CustomFilter(Bytes.toBytes("val-02.07"));
        filters.add(filter2);

        Filter filter3 = new CustomFilter(Bytes.toBytes("val-09.01"));
        filters.add(filter3);

        FilterList filterList = new FilterList(
                FilterList.Operator.MUST_PASS_ONE, filters);

        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ CustomFilterExample
        System.out.println("Results of scan:");
        // vv CustomFilterExample
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }
        }
        scanner.close();
        // ^^ CustomFilterExample

        table.close();
        helper.close();
    }
}
