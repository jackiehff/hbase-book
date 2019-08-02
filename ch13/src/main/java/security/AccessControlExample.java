package security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.security.PrivilegedExceptionAction;
import java.util.List;

/**
 * AccessControlExample Example using the API to handle ACLs
 */
public class AccessControlExample {

    private static TableName tableName;

    public static void main(String[] args) throws Throwable {
        // Login the three user roles: superuser, global admin, and application user.
        final AuthenticatedUser superuser = new AuthenticatedUser(
                "hbase/master-1.hbase.book@HBASE.BOOK", "/tmp/hbase.keytab",
                "Superuser");
        AuthenticatedUser admin = new AuthenticatedUser(
                "hbasebook@HBASE.BOOK", "/tmp/hbasebook.keytab", "Admin");
        AuthenticatedUser app1 = new AuthenticatedUser(
                "app1user1@HBASE.BOOK", "/tmp/app1user1.keytab", "Application");

        tableName = TableName.valueOf("testtable");
        // ^^ AccessControlExample

        System.out.println("Superuser: Preparing table and data...");
        superuser.doAs((PrivilegedExceptionAction<Void>) () -> {
            Configuration conf = superuser.getConfiguration();

            HBaseUtils HBaseUtils = HBaseUtils.getHBaseUtils(conf);
            HBaseUtils.dropTable("testtable");
            HBaseUtils.createTable("testtable", "colfam1", "colfam2");

            System.out.println("Adding rows to table...");
            HBaseUtils.fillTable("testtable", 1, 100, 100, "colfam1", "colfam2");
            HBaseUtils.closeConnection();
            return null;
        });

        System.out.println("Superuser: Checking cluster settings...");
        // vv AccessControlExample
        // co AccessControlExample-02-DoAsSuperuser Run the next commands as the superuser.
        superuser.doAs((PrivilegedExceptionAction<Void>) () -> {
            // co AccessControlExample-03-GetConn Get dedicated connection for authenticated user.
            Connection connection = superuser.getConnection();
            Admin admin1 = connection.getAdmin();
            Table table = connection.getTable(tableName);

            // co AccessControlExample-04-ListCaps List the security capabilities as reported from the Master.
            List<SecurityCapability> sc = admin1.getSecurityCapabilities();
            System.out.println("Superuser: Available security capabilities:");
            for (SecurityCapability cap : sc) {
                System.out.println("  " + cap);
            }

            System.out.println("Superuser: Report AccessController features...");
            // co AccessControlExample-05-PrintAccCtlOpts Report the features enabled regarding access control.
            System.out.println("  Access Controller Running: " +
                    AccessControlClient.isAccessControllerRunning(connection));
            System.out.println("  Authorization Enabled: " +
                    AccessControlClient.isAuthorizationEnabled(connection));
            System.out.println("  Cell Authorization Enabled: " +
                    AccessControlClient.isCellAuthorizationEnabled(connection));

            List<UserPermission> ups = null;
            try {
                // co AccessControlExample-06-PrintPerms Print the current permissions for all tables.
                ups = AccessControlClient.getUserPermissions(connection, ".*");
                System.out.println("Superuser: User permissions:");
                for (UserPermission perm : ups) {
                    System.out.println("  " + perm);
                }
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
            table.close();
            return null;
        });

        // ^^ AccessControlExample
        System.out.println("Superuser: Grant global admin to hbasebook...");
        // vv AccessControlExample
        superuser.grant(admin.getShortUserName(), Permission.Action.values());

        // ^^ AccessControlExample
        System.out.println("Admin & App1: Print permissions...");
        // vv AccessControlExample
        admin.printUserPermissions(null);
        admin.printUserPermissions(".*");
        app1.printUserPermissions(tableName.toString());

        // ^^ AccessControlExample
        System.out.println("Application: Attempting to scan table, will " +
                "return nothing...");
        // When "hbase.security.access.early_out" is set to "true" you will
        // receive an "access denied" error instead!
        // vv AccessControlExample
        app1.scan(tableName, new Scan()); // co AccessControlExample-07-ScanFail The scan will not yield any results as no permissions are granted to the application.
        // ^^ AccessControlExample
        System.out.println("Admin: Grant table read access to application...");
        // vv AccessControlExample
        admin.grant(tableName, app1.getShortUserName(), "colfam1", "col-1",
                Permission.Action.READ);
        admin.printUserPermissions(tableName.toString());
        // ^^ AccessControlExample
        System.out.println("Application: Attempting to scan table again...");
        // vv AccessControlExample
        app1.scan(tableName, new Scan()); // co AccessControlExample-08-ScanSuccess The second scan will return only one column from the otherwise unrestricted scan.

        // ^^ AccessControlExample
        System.out.println("Admin: Grant table write access to application...");
        // vv AccessControlExample
        admin.grant(tableName, app1.getShortUserName(), "colfam1", "col-acl", // co AccessControlExample-09-ColQual Grant write access to the application for a single new column (which does not exist yet).
                Permission.Action.WRITE);
        // ^^ AccessControlExample
        System.out.println("Application: Write into table...");
        // vv AccessControlExample
        Put put = new Put(Bytes.toBytes("row-1"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-acl"),
                Bytes.toBytes("val-acl"));
        app1.put(tableName, put); // co AccessControlExample-10-WriteColumn Insert a value into the granted column.
        // ^^ AccessControlExample
        System.out.println("Application: Scanning table, value not visible...");
        // vv AccessControlExample

        // co AccessControlExample-11-ScanColumn Scanning the table does not show the write-only column, and a direct read of the column will return empty.
        app1.scan(tableName, new Scan().withStartRow(Bytes.toBytes("row-1")).withStopRow(Bytes.toBytes("row-10")));
        // ^^ AccessControlExample
        System.out.println("Application: Attempting to directly access column, " +
                "will return empty...");
        // When "hbase.security.access.early_out" is set to "true" you will
        // receive an "access denied" error instead!
        // vv AccessControlExample
        Get get = new Get(Bytes.toBytes("row-1"));
        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-acl"));
        app1.get(tableName, get);

        // ^^ AccessControlExample
        System.out.println("Admin: Grant read to application for new column...");
        // vv AccessControlExample
        Scan scan = new Scan().withStartRow(Bytes.toBytes("row-1")).withStopRow(Bytes.toBytes("row-10"));
        scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-acl")); // co AccessControlExample-12-GrantCellLvl Grant read access to the application for just the newly added column and access it subsequently.
        admin.grant(tableName, app1.getShortUserName(), scan,
                Permission.Action.READ);
        // ^^ AccessControlExample
        System.out.println("Application: Read new column...");
        // vv AccessControlExample
        app1.scan(tableName, new Scan().withStartRow(Bytes.toBytes("row-1")).withStopRow(Bytes.toBytes("row-10")));

        // ^^ AccessControlExample
        System.out.println("Admin: Put a new cell with read permissions for " +
                "the application...");
        // vv AccessControlExample
        put = new Put(Bytes.toBytes("row-1"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-admin"),
                Bytes.toBytes("val-admin"));
        put.setACL(app1.getShortUserName(),
                new Permission(Permission.Action.READ)); // co AccessControlExample-13-AddCellAcl Admin putting a cell with read permissions for the application.
        admin.put(tableName, put);
        admin.printUserPermissions(tableName.getNameAsString());
        // ^^ AccessControlExample
        System.out.println("Application: Scan table to see if admin column " +
                "is readable...");
        // vv AccessControlExample
        app1.scan(tableName, new Scan().withStartRow(Bytes.toBytes("row-1")).withStopRow(Bytes.toBytes("row-10")));
        // ^^ AccessControlExample
        System.out.println("Application: Directly access column...");
        // vv AccessControlExample
        get = new Get(Bytes.toBytes("row-1"));
        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-admin"));
        app1.get(tableName, get);

        // ^^ AccessControlExample
        System.out.println("Admin: Revoking access for application...");
        // vv AccessControlExample
        admin.revoke(tableName, app1.getShortUserName(), "colfam1", "col-1", // co AccessControlExample-13-RevokeAll Revoke the access permissions previously granted to the application.
                Permission.Action.values());
        admin.revoke(tableName, app1.getShortUserName(), "colfam1", "col-acl",
                Permission.Action.values());
        // ^^ AccessControlExample
        System.out.println("Application: Attempting to scan data...");
        // When "hbase.security.access.early_out" is set to "true" you will
        // receive an "access denied" error instead!
        // vv AccessControlExample
        app1.scan(tableName, new Scan()); // co AccessControlExample-14-ScanFinal Final test if revoking the permissions had an effect. The scan will only return the cell-level granted data (since it was not revoked).
    }
    // ^^ AccessControlExample
}
