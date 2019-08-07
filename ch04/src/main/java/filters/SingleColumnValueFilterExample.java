package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * SingleColumnValueFilterExample Example using a filter to return only rows with a given value in a given column
 */
public class SingleColumnValueFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 10, "colfam1", "colfam2");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("colfam1"),
                    Bytes.toBytes("col-5"),
                    CompareOperator.NOT_EQUAL,
                    new SubstringComparator("val-5"));
            filter.setFilterIfMissing(true);

            Scan scan = new Scan();
            scan.setFilter(filter);
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

            Get get = new Get(Bytes.toBytes("row-6"));
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
