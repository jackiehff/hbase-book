package admin;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import util.HBaseUtils;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * ListTablesExample3 Example listing the existing tables with patterns
 */
public class ListTablesExample3 {

    private static void print(TableName[] tableNames) {
        for (TableName name : tableNames) {
            System.out.println(name);
        }
        System.out.println();
    }

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropNamespace("testspace1", true);
        HBaseUtils.dropNamespace("testspace2", true);
        HBaseUtils.dropTable("testtable3");
        HBaseUtils.createNamespace("testspace1");
        HBaseUtils.createNamespace("testspace2");
        HBaseUtils.createTable("testspace1:testtable1", "colfam1");
        HBaseUtils.createTable("testspace2:testtable2", "colfam1");
        HBaseUtils.createTable("testtable3", "colfam1");

        Admin admin = HBaseUtils.getConnection().getAdmin();

        System.out.println("List: .*");
        // vv ListTablesExample3
        TableName[] names = admin.listTableNames(Pattern.compile(".*"));
        // ^^ ListTablesExample3
        print(names);
        System.out.println("List: .*, including system tables");
        // vv ListTablesExample3
        names = admin.listTableNames(Pattern.compile(".*"), true);
        // ^^ ListTablesExample3
        print(names);

        System.out.println("List: hbase:.*, including system tables");
        // vv ListTablesExample3
        names = admin.listTableNames(Pattern.compile("hbase:.*"), true);
        // ^^ ListTablesExample3
        print(names);

        System.out.println("List: def.*:.*, including system tables");
        // vv ListTablesExample3
        names = admin.listTableNames(Pattern.compile("def.*:.*"), true);
        // ^^ ListTablesExample3
        print(names);

        System.out.println("List: test.*");
        // vv ListTablesExample3
        names = admin.listTableNames(Pattern.compile("test.*"));
        // ^^ ListTablesExample3
        print(names);

        System.out.println("List: .*2, using Pattern");
        // vv ListTablesExample3
        names = admin.listTableNames(Pattern.compile(".*2"));
        // ^^ ListTablesExample3
        print(names);

        System.out.println("List by Namespace: testspace1");
        // vv ListTablesExample3
        names = admin.listTableNamesByNamespace("testspace1");
        // ^^ ListTablesExample3
        print(names);
    }
}
