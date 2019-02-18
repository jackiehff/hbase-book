package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * MutateRowExample Modifies a row with multiple operations
 */
public class MutateRowExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", 3, "colfam1");
        helper.put("testtable",
                new String[]{"row1"},
                new String[]{"colfam1"},
                new String[]{"qual1", "qual2", "qual3"},
                new long[]{1, 2, 3},
                new String[]{"val1", "val2", "val3"});
        System.out.println("Before delete call...");
        helper.dump("testtable", new String[]{"row1"}, null, null);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                4, Bytes.toBytes("val99"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual4"),
                4, Bytes.toBytes("val100"));

        Delete delete = new Delete(Bytes.toBytes("row1"));
        delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));

        RowMutations mutations = new RowMutations(Bytes.toBytes("row1"));
        mutations.add((Mutation) put);
        mutations.add((Mutation) delete);

        table.mutateRow(mutations);

        table.close();
        connection.close();
        System.out.println("After mutate call...");
        helper.dump("testtable", new String[]{"row1"}, null, null);
        helper.close();
    }
}
