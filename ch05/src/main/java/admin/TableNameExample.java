package admin;

import org.apache.hadoop.hbase.TableName;

/**
 * TableNameExample Example how to create a TableName in code
 */
public class TableNameExample {

    private static void print(String tableName) {
        print(null, tableName);
    }

    private static void print(String namespace, String tableName) {
        System.out.print("Given Namespace: " + namespace + ", Tablename: " + tableName + " -> ");
        try {
            System.out.println(namespace != null ?
                    TableName.valueOf(namespace, tableName) :
                    TableName.valueOf(tableName));
        } catch (Exception e) {
            System.out.println(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        print("testtable");
        print("testspace:testtable");
        print("testspace", "testtable");
        print("testspace", "te_st-ta.ble");
        print("", "TestTable-100");
        print("tEsTsPaCe", "te_st-table");

        print("");

        // VALID_NAMESPACE_REGEX = "(?:[a-zA-Z_0-9]+)";
        // VALID_TABLE_QUALIFIER_REGEX = "(?:[a-zA-Z_0-9][a-zA-Z_0-9-.]*)";
        print(".testtable");
        print("te_st-space", "te_st-table");
        print("tEsTsPaCe", "te_st-table@dev");
    }
}
