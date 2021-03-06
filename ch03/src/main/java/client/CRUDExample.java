package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * CRUDExample Example application using all of the basic access methods (v1.0 and later)
 */
public class CRUDExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1", "colfam2");

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            // Put操作
            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
            put.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));
            table.put(put);

            // Scan 操作
            Scan scan = new Scan();
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    while (result.advance()) {
                        System.out.println("Cell: " + result.current());
                    }
                }
            }

            // Get操作
            Get get = new Get(Bytes.toBytes("row1"));
            get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
            Result result = table.get(get);
            System.out.println("Get result: " + result);
            byte[] val = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
            System.out.println("Value only: " + Bytes.toString(val));

            // Delete操作
            Delete delete = new Delete(Bytes.toBytes("row1"));
            delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
            table.delete(delete);

            // Scan 操作
            Scan scan2 = new Scan();
            ResultScanner scanner2 = table.getScanner(scan2);
            for (Result result2 : scanner2) {
                System.out.println("Scan: " + result2);
            }
        }

        HBaseUtils.closeConnection();
    }
}
