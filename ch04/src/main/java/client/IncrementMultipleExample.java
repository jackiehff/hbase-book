package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * IncrementMultipleExample Example incrementing multiple counters in one row
 */
public class IncrementMultipleExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "daily", "weekly");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Increment increment1 = new Increment(Bytes.toBytes("20150101"));

            increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 1);
            // co IncrementMultipleExample-1-Incr1 Increment the counters with various values.
            increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
            increment1.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"), 10);
            increment1.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), 10);
            // ^^ IncrementMultipleExample
            Map<byte[], NavigableMap<byte[], Long>> longs =
                    increment1.getFamilyMapOfLongs();
            for (byte[] family : longs.keySet()) {
                System.out.println("Increment #1 - family: " + Bytes.toString(family));
                NavigableMap<byte[], Long> longcols = longs.get(family);
                for (byte[] column : longcols.keySet()) {
                    System.out.print("  column: " + Bytes.toString(column));
                    System.out.println(" - value: " + longcols.get(column));
                }
            }

            // co IncrementMultipleExample-2-Incr2 Call the actual increment method with the above counter updates and receive the results.
            Result result1 = table.increment(increment1);
            for (Cell cell : result1.rawCells()) {
                // co IncrementMultipleExample-3-Dump1 Print the cell and returned counter value.
                System.out.println("Cell: " + cell + " Value: " + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }

            Increment increment2 = new Increment(Bytes.toBytes("20150101"));

            increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 5);
            // co IncrementMultipleExample-4-Incr3 Use positive, negative, and zero increment values to achieve the wanted counter changes.
            increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
            increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"), 0);
            increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), -5);

            Result result2 = table.increment(increment2);

            for (Cell cell : result2.rawCells()) {
                System.out.println("Cell: " + cell + " Value: " + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
        }
        HBaseUtils.closeConnection();
    }
}
