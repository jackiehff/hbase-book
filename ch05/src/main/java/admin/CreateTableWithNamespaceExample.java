package admin;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * CreateTableWithNamespaceExample Example using the administrative API to create a table with a custom namespace
 */
public class CreateTableWithNamespaceExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");
        Admin admin = helper.getConnection().getAdmin();

        NamespaceDescriptor namespace = NamespaceDescriptor.create("testspace").build();
        admin.createNamespace(namespace);

        TableName tableName = TableName.valueOf("testspace", "testtable");
        HTableDescriptor desc = new HTableDescriptor(tableName);

        HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("colfam1"));
        desc.addFamily(coldef);

        admin.createTable(desc);

        boolean avail = admin.isTableAvailable(tableName);
        System.out.println("Table available: " + avail);
    }
}
