package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * IncrementSingleExample Example using the single counter increment methods
 */
public class IncrementSingleExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "daily");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            // co IncrementSingleExample-1-Incr1 Increase counter by one.
            long cnt1 = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
            // co IncrementSingleExample-2-Incr2 Increase counter by one a second time.
            long cnt2 = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
            // co IncrementSingleExample-3-GetCurrent Get current value of the counter without increasing it.
            long current = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), 0);
            // co IncrementSingleExample-4-Decr1 Decrease counter by one.
            long cnt3 = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), -1);
            System.out.println("cnt1: " + cnt1 + ", cnt2: " + cnt2 + ", current: " + current + ", cnt3: " + cnt3);
        }
        HBaseUtils.closeConnection();
    }
}
