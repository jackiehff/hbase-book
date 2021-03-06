package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GetListExample Example of retrieving data from HBase using lists of Get instances
 */
public class GetListExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            byte[] cf1 = Bytes.toBytes("colfam1");
            byte[] qf1 = Bytes.toBytes("qual1");
            // co GetListExample-1-Prepare Prepare commonly used byte arrays.
            byte[] qf2 = Bytes.toBytes("qual2");
            byte[] row1 = Bytes.toBytes("row1");
            byte[] row2 = Bytes.toBytes("row2");

            // co GetListExample-2-CreateList Create a list that holds the Get instances.
            List<Get> gets = new ArrayList<>();

            Get get1 = new Get(row1);
            get1.addColumn(cf1, qf1);
            gets.add(get1);

            Get get2 = new Get(row2);
            // co GetListExample-3-AddGets Add the Get instances to the list.
            get2.addColumn(cf1, qf1);
            gets.add(get2);

            Get get3 = new Get(row2);
            get3.addColumn(cf1, qf2);
            gets.add(get3);

            // co GetListExample-4-DoGet Retrieve rows with selected columns from HBase.
            Result[] results = table.get(gets);

            System.out.println("First iteration...");
            for (Result result : results) {
                String row = Bytes.toString(result.getRow());
                System.out.print("Row: " + row + " ");
                byte[] val = null;
                // co GetListExample-5-GetValue1 Iterate over results and check what values are available.
                if (result.containsColumn(cf1, qf1)) {
                    val = result.getValue(cf1, qf1);
                    System.out.println("Value: " + Bytes.toString(val));
                }
                if (result.containsColumn(cf1, qf2)) {
                    val = result.getValue(cf1, qf2);
                    System.out.println("Value: " + Bytes.toString(val));
                }
            }

            System.out.println("Second iteration...");
            for (Result result : results) {
                // co GetListExample-6-GetValue2 Iterate over results again, printing out all values.
                for (Cell cell : result.listCells()) {
                    // co GetListExample-7-GetValue2 Two different ways to access the cell data.
                    System.out.println(
                            "Row: " + Bytes.toString(
                                    cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) +
                                    " Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

            System.out.println("Third iteration...");
            for (Result result : results) {
                System.out.println(result);
            }
        }

        HBaseUtils.closeConnection();
    }
}
