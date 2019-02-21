package client;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * DeleteListExample Example application deleting lists of data from HBase
 */
public class DeleteListExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", 100, "colfam1", "colfam2");
        helper.put("testtable",
                new String[]{"row1"},
                new String[]{"colfam1", "colfam2"},
                new String[]{"qual1", "qual1", "qual2", "qual2", "qual3", "qual3"},
                new long[]{1, 2, 3, 4, 5, 6},
                new String[]{"val1", "val2", "val3", "val4", "val5", "val6"});
        helper.put("testtable",
                new String[]{"row2"},
                new String[]{"colfam1", "colfam2"},
                new String[]{"qual1", "qual1", "qual2", "qual2", "qual3", "qual3"},
                new long[]{1, 2, 3, 4, 5, 6},
                new String[]{"val1", "val2", "val3", "val4", "val5", "val6"});
        helper.put("testtable",
                new String[]{"row3"},
                new String[]{"colfam1", "colfam2"},
                new String[]{"qual1", "qual1", "qual2", "qual2", "qual3", "qual3"},
                new long[]{1, 2, 3, 4, 5, 6},
                new String[]{"val1", "val2", "val3", "val4", "val5", "val6"});
        System.out.println("Before delete call...");
        helper.dump("testtable", new String[]{"row1", "row2", "row3"}, null, null);

        Table table = helper.getTable("testtable");

        // co DeleteListExample-1-CreateList Create a list that holds the Delete instances.
        List<Delete> deletes = new ArrayList<>();

        Delete delete1 = new Delete(Bytes.toBytes("row1"));
        // co DeleteListExample-2-SetTS Set timestamp for row deletes.
        delete1.setTimestamp(4);
        deletes.add(delete1);

        Delete delete2 = new Delete(Bytes.toBytes("row2"));
        // co DeleteListExample-3-DelColNoTS Delete the latest version only in one column.
        delete2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        // co DeleteListExample-4-DelColsTS Delete the given and all older versions in another column.
        delete2.addColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"), 5);
        deletes.add(delete2);

        Delete delete3 = new Delete(Bytes.toBytes("row3"));
        // co DeleteListExample-5-AddCol Delete entire family, all columns and versions.
        delete3.addFamily(Bytes.toBytes("colfam1"));
        // co DeleteListExample-6-AddCol Delete the given and all older versions in the entire column family, i.e., from all columns therein.
        delete3.addFamily(Bytes.toBytes("colfam2"), 3);
        deletes.add(delete3);

        // co DeleteListExample-7-DoDel Delete the data from multiple rows the HBase table.
        table.delete(deletes);

        table.close();
        System.out.println("After delete call...");
        helper.dump("testtable", new String[]{"row1", "row2", "row3"}, null, null);
        helper.close();
    }
}
