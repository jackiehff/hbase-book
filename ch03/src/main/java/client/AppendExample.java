package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * AppendExample Example application appending data to a column in  HBase
 */
public class AppendExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, 100, "colfam1", "colfam2");
        HBaseUtils.put(HBaseConstants.TEST_TABLE,
                new String[]{"row1"},
                new String[]{"colfam1"},
                new String[]{"qual1"},
                new long[]{1},
                new String[]{"oldvalue"});
        System.out.println("Before append call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Append append = new Append(Bytes.toBytes("row1"));
            append.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("newvalue"));
            append.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("anothervalue"));
            table.append(append);
        }
        System.out.println("After append call...");
        HBaseUtils.dump(HBaseConstants.TEST_TABLE, new String[]{"row1"}, null, null);
        HBaseUtils.closeConnection();
    }
}
