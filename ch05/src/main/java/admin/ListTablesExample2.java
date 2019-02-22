package admin;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * ListTablesExample2 Example listing the existing tables with patterns
 */
public class ListTablesExample2 {

    private static void print(List<TableDescriptor> descriptors) {
        for (TableDescriptor htd : descriptors) {
            System.out.println(htd.getTableName());
        }
        System.out.println();
    }

    public static void main(String[] args) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper();
        helper.dropNamespace("testspace1", true);
        helper.dropNamespace("testspace2", true);
        helper.dropTable("testtable3");
        helper.createNamespace("testspace1");
        helper.createNamespace("testspace2");
        helper.createTable("testspace1:testtable1", "colfam1");
        helper.createTable("testspace2:testtable2", "colfam1");
        helper.createTable("testtable3", "colfam1");

        Admin admin = helper.getConnection().getAdmin();

        System.out.println("List: .*");
        // vv ListTablesExample2
        List<TableDescriptor> htds = admin.listTableDescriptors(Pattern.compile(".*"));
        // ^^ ListTablesExample2
        print(htds);
        System.out.println("List: .*, including system tables");
        // vv ListTablesExample2
        htds = admin.listTableDescriptors(Pattern.compile(".*"), true);
        // ^^ ListTablesExample2
        print(htds);

        System.out.println("List: hbase:.*, including system tables");
        // vv ListTablesExample2
        htds = admin.listTableDescriptors(Pattern.compile("hbase:.*"), true);
        // ^^ ListTablesExample2
        print(htds);

        System.out.println("List: def.*:.*, including system tables");
        // vv ListTablesExample2
        htds = admin.listTableDescriptors(Pattern.compile("def.*:.*"), true);
        // ^^ ListTablesExample2
        print(htds);

        System.out.println("List: test.*");
        // vv ListTablesExample2
        htds = admin.listTableDescriptors(Pattern.compile("test.*"));
        // ^^ ListTablesExample2
        print(htds);

        System.out.println("List: .*2, using Pattern");
        // vv ListTablesExample2
        htds = admin.listTableDescriptors(Pattern.compile(".*2"));
        // ^^ ListTablesExample2
        print(htds);

        System.out.println("List by Namespace: testspace1");
        // vv ListTablesExample2
        htds = admin.listTableDescriptorsByNamespace(Bytes.toBytes("testspace1"));
        // ^^ ListTablesExample2
        print(htds);
    }
}
