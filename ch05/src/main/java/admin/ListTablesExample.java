package admin;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import util.HBaseHelper;

import java.io.IOException;
import java.util.List;

/**
 * ListTablesExample Example listing the existing tables and their descriptors
 */
public class ListTablesExample {

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropTable("testtable1");
        helper.dropTable("testtable2");
        helper.dropTable("testtable3");
        helper.createTable("testtable1", "colfam1", "colfam2", "colfam3");
        helper.createTable("testtable2", "colfam1", "colfam2", "colfam3");
        helper.createTable("testtable3", "colfam1", "colfam2", "colfam3");

        // vv ListTablesExample
        Admin admin = helper.getConnection().getAdmin();

        List<TableDescriptor> htds = admin.listTableDescriptors();
        System.out.println("Printing all tables...");
        for (TableDescriptor htd : htds) {
            System.out.println(htd);
        }

        TableDescriptor htd1 = admin.getDescriptor(TableName.valueOf("testtable1"));
        // ^^ ListTablesExample
        System.out.println("Printing testtable1...");
        // vv ListTablesExample
        System.out.println(htd1);

        TableDescriptor htd2 = admin.getDescriptor(TableName.valueOf("testtable10"));
        // ^^ ListTablesExample
        System.out.println("Printing testtable10...");
        // vv ListTablesExample
        System.out.println(htd2);
        // ^^ ListTablesExample
    }
}
