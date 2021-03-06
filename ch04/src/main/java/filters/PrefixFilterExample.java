package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * PrefixFilterExample Example using the prefix based filter
 */
public class PrefixFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 10, "colfam1", "colfam2");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Filter filter = new PrefixFilter(Bytes.toBytes("row-1"));

            Scan scan = new Scan();
            scan.setFilter(filter);
            try (ResultScanner scanner = table.getScanner(scan)) {
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
        }

        HBaseUtils.closeConnection();
    }
}
