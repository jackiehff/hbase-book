package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * GetCloneExample Example application retrieving data from HBase
 */
public class GetCloneExample {

    public static void main(String[] args) throws IOException {
        if (!HBaseUtils.existsTable(HBaseConstants.TEST_TABLE)) {
            HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        }

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            Get get1 = new Get(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));

            Get get2 = new Get(get1);
            Result result = table.get(get2);
            System.out.println("Result : " + result);
        }

        HBaseUtils.closeConnection();
    }
}
