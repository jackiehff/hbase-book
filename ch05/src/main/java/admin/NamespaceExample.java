package admin;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import util.HBaseUtils;

import java.io.IOException;

/**
 * NamespaceExample Example using the administrative API to create etc. a namespace
 */
public class NamespaceExample {

    public static void main(String[] args) throws IOException {
        Admin admin = HBaseUtils.getConnection().getAdmin();
        // ^^ NamespaceExample
        try {
            TableName[] tbls = admin.listTableNamesByNamespace("testspace");
            for (TableName tbl : tbls) {
                admin.disableTable(tbl);
                admin.deleteTable(tbl);
            }
            admin.deleteNamespace("testspace");
        } catch (IOException e) {
            // ignore
        }
        // vv NamespaceExample
        NamespaceDescriptor namespace =
                NamespaceDescriptor.create("testspace").build();
        admin.createNamespace(namespace);

        NamespaceDescriptor namespace2 =
                admin.getNamespaceDescriptor("testspace");
        System.out.println("Simple Namespace: " + namespace2);

        NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor nd : list) {
            System.out.println("List Namespace: " + nd);
        }

        NamespaceDescriptor namespace3 =
                NamespaceDescriptor.create("testspace")
                        .addConfiguration("Description", "Test Namespace")
                        .build();
        admin.modifyNamespace(namespace3);

        NamespaceDescriptor namespace4 =
                admin.getNamespaceDescriptor("testspace");
        System.out.println("Custom Namespace: " + namespace4);

        admin.deleteNamespace("testspace");

        NamespaceDescriptor[] list2 = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor nd : list2) {
            System.out.println("List Namespace: " + nd);
        }
        // ^^ NamespaceExample
    }
}
