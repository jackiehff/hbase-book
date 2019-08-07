package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FuzzyRowFilterExample Example filtering by column prefix
 */
public class FuzzyRowFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 20, 10, 2, true, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            List<Pair<byte[], byte[]>> keys = new ArrayList<>();
            keys.add(new Pair<>(
                    Bytes.toBytes("row-?5"), new byte[]{0, 0, 0, 0, 1, 0}));
            Filter filter = new FuzzyRowFilter(keys);

            Scan scan = new Scan()
                    .addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5"))
                    .setFilter(filter);
            try (ResultScanner scanner = table.getScanner(scan)) {
                System.out.println("Results of scan:");
                for (Result result : scanner) {
                    System.out.println(result);
                }
            }
        }

        HBaseUtils.closeConnection();
    }
}
