package filters;

// cc MultipleColumnPrefixFilterExample Example filtering by column prefix

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class MultipleColumnPrefixFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 30, 50, 0, true, "colfam1");

        Table table = helper.getTable("testtable");
        // vv MultipleColumnPrefixFilterExample
        Filter filter = new MultipleColumnPrefixFilter(new byte[][]{
                Bytes.toBytes("col-1"), Bytes.toBytes("col-2")
        });

        Scan scan = new Scan()
                .setRowPrefixFilter(Bytes.toBytes("row-1")) // co MultipleColumnPrefixFilterExample-1-Row Limit to rows starting with a specific prefix.
                .setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ MultipleColumnPrefixFilterExample
        System.out.println("Results of scan:");
        // vv MultipleColumnPrefixFilterExample
        for (Result result : scanner) {
            System.out.print(Bytes.toString(result.getRow()) + ": ");
            for (Cell cell : result.rawCells()) {
                System.out.print(Bytes.toString(cell.getQualifierArray(),
                        cell.getQualifierOffset(), cell.getQualifierLength()) + ", ");
            }
            System.out.println();
        }
        scanner.close();

        table.close();
        helper.close();
    }
}
