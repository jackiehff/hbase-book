package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * FirstKeyOnlyFilterExample Only returns the first found cell from each row
 */
public class FirstKeyOnlyFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTableRandom(HBaseConstants.TEST_TABLE, /* row */ 1, 30, 0,
                /* col */ 1, 30, 0,  /* val */ 0, 100, 0, true, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Filter filter = new FirstKeyOnlyFilter();

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            // ^^ FirstKeyOnlyFilterExample
            System.out.println("Results of scan:");
            // vv FirstKeyOnlyFilterExample
            int rowCount = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                }
                rowCount++;
            }
            System.out.println("Total num of rows: " + rowCount);
            scanner.close();
        }
        HBaseUtils.closeConnection();
    }
}
