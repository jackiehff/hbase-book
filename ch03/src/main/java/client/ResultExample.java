package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * ResultExample Retrieve results from server and dump content
 */
public class ResultExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val1"));
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                    Bytes.toBytes("val2"));
            table.put(put);

            Get get = new Get(Bytes.toBytes("row1"));
            Result result1 = table.get(get);
            System.out.println(result1);

            Result result2 = Result.EMPTY_RESULT;
            System.out.println(result2);

            result2.copyFrom(result1);
            System.out.println(result2);
        }
        HBaseUtils.closeConnection();
    }
}
