package filters;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * ValueFilterExample Example using the value based filter
 */
public class ValueFilterExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));
        // vv ValueFilterExample
        Filter filter = new ValueFilter(CompareOperator.EQUAL, // co ValueFilterExample-1-Filter Create filter, while specifying the comparison operator and comparator.
                new SubstringComparator(".4"));

        Scan scan = new Scan();
        // co ValueFilterExample-2-SetFilter Set filter for the scan.
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ ValueFilterExample
        System.out.println("Results of scan:");
        // vv ValueFilterExample
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                // co ValueFilterExample-3-Print1 Print out value to check that filter works.
                System.out.println("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        }
        scanner.close();

        Get get = new Get(Bytes.toBytes("row-5"));
        // co ValueFilterExample-4-SetFilter2 Assign same filter to Get instance.
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println("Result of get: ");
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength()));
        }
    }
}
