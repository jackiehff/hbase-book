package admin;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import util.HBaseHelper;

import java.io.IOException;

/**
 * ModifyTableExample Example modifying the structure of an existing table
 */
public class ModifyTableExample {

    public static void main(String[] args) throws IOException, InterruptedException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable");

        Admin admin = helper.getConnection().getAdmin();
        TableName tableName = TableName.valueOf("testtable");

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .setValue("Description", "Chapter 5 - ModifyTableExample: Original Table")
                .build();

        // co ModifyTableExample-1-CreateTable Create the table with the original structure and 50 regions.
        admin.createTable(tableDescriptor, Bytes.toBytes(1L), Bytes.toBytes(10000L), 50);
        // co ModifyTableExample-2-SchemaUpdate Get schema, update by adding a new family and changing the maximum file size property.
        TableDescriptor htd1 = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam2")).build())
                .setMaxFileSize(1024 * 1024 * 1024L)
                .setValue("Description", "Chapter 5 - ModifyTableExample: Original Table")
                .build();


        admin.disableTable(tableName);
        // co ModifyTableExample-3-ChangeTable Disable and modify the table.
        admin.modifyTable(htd1);

        // co ModifyTableExample-4-Pair Create a status number pair to start the loop.
        Pair<Integer, Integer> status = new Pair<Integer, Integer>() {{
            setFirst(50);
            setSecond(50);
        }};
        for (int i = 0; status.getFirst() != 0 && i < 500; i++) {
            // co ModifyTableExample-5-Loop Loop over status until all regions are updated, or 500 seconds have been exceeded.
            status = admin.getAlterStatus(tableDescriptor.getTableName());
            if (status.getSecond() != 0) {
                int pending = status.getSecond() - status.getFirst();
                System.out.println(pending + " of " + status.getSecond()
                        + " regions updated.");
                Thread.sleep(1 * 1000L);
            } else {
                System.out.println("All regions updated.");
                break;
            }
        }
        if (status.getFirst() != 0) {
            throw new IOException("Failed to update regions after 500 seconds.");
        }

        admin.enableTable(tableName);

        TableDescriptor htd2 = admin.getDescriptor(tableName);
        // co ModifyTableExample-6-Verify Check if the table schema matches the new one created locally.
        System.out.println("Equals: " + htd1.equals(htd2));
        System.out.println("New schema: " + htd2);
        // ^^ ModifyTableExample
    }
}
