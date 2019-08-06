package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * MultipleColumnPrefixFilterExample Example filtering by column prefix
 */
public class MultipleColumnPrefixFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 30, 50, 0, true, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Filter filter = new MultipleColumnPrefixFilter(new byte[][]{
                    Bytes.toBytes("col-1"), Bytes.toBytes("col-2")
            });

            Scan scan = new Scan()
                    .setRowPrefixFilter(Bytes.toBytes("row-1")) // co MultipleColumnPrefixFilterExample-1-Row Limit to rows starting with a specific prefix.
                    .setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("Results of scan:");
            for (Result result : scanner) {
                System.out.print(Bytes.toString(result.getRow()) + ": ");
                for (Cell cell : result.rawCells()) {
                    System.out.print(Bytes.toString(cell.getQualifierArray(),
                            cell.getQualifierOffset(), cell.getQualifierLength()) + ", ");
                }
                System.out.println();
            }
            scanner.close();
        }

        HBaseUtils.closeConnection();
    }
}
