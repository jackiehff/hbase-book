package filters;


import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ValueFilterExample Example using the value based filter
 */
public class ValueFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 10, "colfam1", "colfam2");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Filter filter = new ValueFilter(CompareOperator.EQUAL, // co ValueFilterExample-1-Filter Create filter, while specifying the comparison operator and comparator.
                    new SubstringComparator(".4"));

            Scan scan = new Scan();
            // co ValueFilterExample-2-SetFilter Set filter for the scan.
            scan.setFilter(filter);
            try(ResultScanner scanner = table.getScanner(scan)) {
                System.out.println("Results of scan:");
                for (Result result : scanner) {
                    for (Cell cell : result.rawCells()) {
                        // co ValueFilterExample-3-Print1 Print out value to check that filter works.
                        System.out.println("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
                    }
                }
            }

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

        HBaseUtils.closeConnection();
    }
}
