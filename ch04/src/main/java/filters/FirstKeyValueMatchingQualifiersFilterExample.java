package filters;

// cc FirstKeyValueMatchingQualifiersFilterExample Returns all columns, or up to the first found reference qualifier, for each row

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyValueMatchingQualifiersFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FirstKeyValueMatchingQualifiersFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTableRandom("testtable", /* row */ 1, 50, 0,
                /* col */ 1, 10, 0,  /* val */ 0, 100, 0, true, "colfam1");

        Table table = helper.getTable("testtable");
        // vv FirstKeyValueMatchingQualifiersFilterExample
        Set<byte[]> quals = new HashSet<>();
        quals.add(Bytes.toBytes("col-2"));
        quals.add(Bytes.toBytes("col-4"));
        quals.add(Bytes.toBytes("col-6"));
        quals.add(Bytes.toBytes("col-8"));
        Filter filter = new FirstKeyValueMatchingQualifiersFilter(quals);

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        // ^^ FirstKeyValueMatchingQualifiersFilterExample
        System.out.println("Results of scan:");
        // vv FirstKeyValueMatchingQualifiersFilterExample
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
}
