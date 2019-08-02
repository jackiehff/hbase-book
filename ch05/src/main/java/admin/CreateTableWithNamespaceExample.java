package admin;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * CreateTableWithNamespaceExample Example using the administrative API to create a table with a custom namespace
 */
public class CreateTableWithNamespaceExample {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        Admin admin = HBaseUtils.getConnection().getAdmin();

        NamespaceDescriptor namespace = NamespaceDescriptor.create("testspace").build();
        admin.createNamespace(namespace);

        TableName tableName = TableName.valueOf("testspace", "testtable");
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .build();
        admin.createTable(tableDescriptor);

        boolean avail = admin.isTableAvailable(tableName);
        System.out.println("Table available: " + avail);
    }
}
