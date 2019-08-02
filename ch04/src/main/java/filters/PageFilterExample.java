package filters;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * PageFilterExample Example using a filter to paginate through rows
 */
public class PageFilterExample {

    private static final byte[] POSTFIX = new byte[]{0x00};

    public static void main(String[] args) throws IOException {

        HBaseUtils.dropTable("testtable");
        HBaseUtils.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable("testtable", 1, 1000, 10, "colfam1");

        Table table = HBaseUtils.getTable("testtable");

        // vv PageFilterExample
        Filter filter = new PageFilter(15);

        int totalRows = 0;
        byte[] lastRow = null;
        while (true) {
            Scan scan = new Scan();
            scan.setFilter(filter);
            if (lastRow != null) {
                byte[] startRow = Bytes.add(lastRow, POSTFIX);
                System.out.println("start row: " +
                        Bytes.toStringBinary(startRow));
                scan.withStartRow(startRow);
            }
            ResultScanner scanner = table.getScanner(scan);
            int localRows = 0;
            Result result;
            while ((result = scanner.next()) != null) {
                System.out.println(localRows++ + ": " + result);
                totalRows++;
                lastRow = result.getRow();
            }
            scanner.close();
            if (localRows == 0) {
                break;
            }
        }
        System.out.println("total rows: " + totalRows);

        table.close();
        HBaseUtils.closeConnection();
    }
}
