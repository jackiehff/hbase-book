package datatasks;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

// cc RenameTableExample An example how to rename a table using the API.
public class RenameTableExample {

    // vv RenameTableExample
    private static void renameTable(Admin admin, TableName oldName, TableName newName)
            throws IOException {
        String snapshotName = "SnapRename-" + System.currentTimeMillis(); // co RenameTableExample-01-RandomName Create a unique (timestamped) snapshot name avoiding collisions.
        admin.disableTable(oldName); // co RenameTableExample-02-DisableTable Disable table to avoid any concurrent writes. This is optional and could be done on demand.
        admin.snapshot(snapshotName, oldName); // co RenameTableExample-03-TakeSnap Take the snapshot of the table.
        if (admin.tableExists(newName)) { // co RenameTableExample-04-RemoveNew Check if the new table name already exists and, if so, remove it first.
            admin.disableTable(newName);
            admin.deleteTable(newName);
        }
        try {
            admin.cloneSnapshot(snapshotName, newName); // co RenameTableExample-05-RestoreSnap Restore the snapshot, and remove the old table.
            admin.deleteTable(oldName);
        } finally {
            admin.deleteSnapshot(snapshotName); // co RenameTableExample-06-DropSnap Drop the snapshot to clean up behind the rename operation.
        }
    }

    private static void printFirstValue(Table table) throws IOException {
        System.out.println("Table: " + table.getName());
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        Result res = results.next();
        CellScanner scanner = res.cellScanner();
        if (scanner.advance()) {
            Cell cell = scanner.current();
            System.out.println("Value: " + Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength()));
        } else {
            System.out.println("No data");
        }
    }

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 100, 100, "colfam1");
        // vv RenameTableExample
        Admin admin = helper.getConnection().getAdmin();
        TableName name = TableName.valueOf("testtable");

        // co RenameTableExample-07-Test1 Check the content of the original table. The helper method (see full source code) prints the first value of the first row.
        Table table = helper.getTable(name);
        printFirstValue(table);

        TableName rename = TableName.valueOf("newtesttable");
        // co RenameTableExample-08-Rename Rename the table calling the above method.
        renameTable(admin, name, rename);

        // co RenameTableExample-09-Test2 Perform another check on the new table to see if we get the same first value of the first row back.
        Table newTable = helper.getTable(rename);
        printFirstValue(newTable);

        table.close();
        newTable.close();
        admin.close();
        helper.close();
    }
}
