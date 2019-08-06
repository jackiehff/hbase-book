package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GetListErrorExample Example trying to read an erroneous column family
 */
public class GetListErrorExample {

    public static void main(String[] args) throws IOException {
        if (!HBaseUtils.existsTable(HBaseConstants.TEST_TABLE)) {
            HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");
        }

        try (Table table = HBaseUtils.getTable(HBaseConstants.TEST_TABLE)) {
            byte[] cf1 = Bytes.toBytes("colfam1");
            byte[] qf1 = Bytes.toBytes("qual1");
            byte[] qf2 = Bytes.toBytes("qual2");
            byte[] row1 = Bytes.toBytes("row1");
            byte[] row2 = Bytes.toBytes("row2");

            List<Get> gets = new ArrayList<>();

            Get get1 = new Get(row1);
            get1.addColumn(cf1, qf1);
            gets.add(get1);

            Get get2 = new Get(row2);
            // co GetListErrorExample-1-AddGets Add the Get instances to the list.
            get2.addColumn(cf1, qf1);
            gets.add(get2);

            Get get3 = new Get(row2);
            get3.addColumn(cf1, qf2);
            gets.add(get3);

            Get get4 = new Get(row2);
            get4.addColumn(Bytes.toBytes("BOGUS"), qf2);
            // co GetListErrorExample-2-AddBogus Add the bogus column family get.
            gets.add(get4);
            // co GetListErrorExample-3-Error An exception is thrown and the process is aborted.
            Result[] results = table.get(gets);
            // co GetListErrorExample-4-SOUT This line will never reached!
            System.out.println("Result count: " + results.length);
        }
        HBaseUtils.closeConnection();
    }
}
