package filters;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * FamilyFilterExample Example using a filter to include only specific column families
 */
public class FamilyFilterExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2", "colfam3", "colfam4");
        System.out.println("Adding rows to table...");
        HBaseUtils.fillTable(HBaseConstants.TEST_TABLE, 1, 10, 2, "colfam1", "colfam2", "colfam3", "colfam4");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Filter filter1 = new FamilyFilter(CompareOperator.LESS, // co FamilyFilterExample-1-Filter Create filter, while specifying the comparison operator and comparator.
                    new BinaryComparator(Bytes.toBytes("colfam3")));

            Scan scan = new Scan();
            scan.setFilter(filter1);
            try(ResultScanner scanner = table.getScanner(scan)) { // co FamilyFilterExample-2-Scan Scan over table while applying the filter.
                System.out.println("Scanning table... ");
                for (Result result : scanner) {
                    System.out.println(result);
                }
            }

            Get get1 = new Get(Bytes.toBytes("row-5"));
            get1.setFilter(filter1);
            Result result1 = table.get(get1); // co FamilyFilterExample-3-Get Get a row while applying the same filter.
            System.out.println("Result of get(): " + result1);

            Filter filter2 = new FamilyFilter(CompareOperator.EQUAL,
                    new BinaryComparator(Bytes.toBytes("colfam3")));
            Get get2 = new Get(Bytes.toBytes("row-5")); // co FamilyFilterExample-4-Mismatch Create a filter on one column family while trying to retrieve another.
            get2.addFamily(Bytes.toBytes("colfam1"));
            get2.setFilter(filter2);
            Result result2 = table.get(get2); // co FamilyFilterExample-5-Get2 Get the same row while applying the new filter, this will return "NONE".
            System.out.println("Result of get(): " + result2);
        }

        HBaseUtils.closeConnection();
    }
}
