package admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.quotas.*;
import util.HBaseHelper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * QuotaExample Example using the quota related classes and API
 */
public class QuotaExample {

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.setInt("hbase.client.retries.number", 1);

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropNamespace("bar", true);
        helper.dropNamespace("foo", true);
        helper.createNamespace("bar");
        helper.createNamespace("foo");
        helper.createTable("foo:unlimited", "cf1");
        helper.createTable("foo:limited", "cf1");
        helper.createTable("bar:limited", "cf1");
        System.out.println("Adding rows to tables...");
        helper.fillTable("foo:limited", 1, 10, 1, "cf1");

        // vv QuotaExample
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName fooLimited = TableName.valueOf("foo:limited");
        // co QuotaExample-1-Names Create the table name instances for the test tables.
        TableName fooUnlimited = TableName.valueOf("foo:unlimited");
        TableName barLimited = TableName.valueOf("bar:limited");

        // co QuotaExample-2-Tables Create a reference to the table and the admin API.
        Table table = connection.getTable(fooLimited);
        Admin admin = connection.getAdmin();

        // co QuotaExample-3-Quota1 Configure a quota setting record at the table level, and assign it.
        QuotaSettings qs = QuotaSettingsFactory.throttleTable(fooLimited, ThrottleType.READ_NUMBER, 5, TimeUnit.DAYS);
        admin.setQuota(qs);

        Scan scan = new Scan();
        scan.setCaching(1);
        // co QuotaExample-4-Scan Scan the table to measure the effect of the quota.
        ResultScanner scanner = table.getScanner(scan);
        int numRows = 0;
        try {
            for (Result res : scanner) {
                System.out.println(res);
                numRows++;
            }
        } catch (Exception e) {
            System.out.println("Error occurred: " + e.getMessage());
        }
        System.out.printf("Number of rows: " + numRows);
        scanner.close();

        // co QuotaExample-5-Quota2 Configure another quota settings record, this time on the namespace level, and assign it.
        qs = QuotaSettingsFactory.throttleUser("hbasebook", "bar",
                ThrottleType.REQUEST_NUMBER, 5, TimeUnit.SECONDS);
        admin.setQuota(qs);

        QuotaFilter qf = new QuotaFilter();
        // ^^ QuotaExample
        qf.addTypeFilter(QuotaType.THROTTLE);
        //qf.setNamespaceFilter("foo");
        // co QuotaExample-6-GetQuotas Configure a filter, get a retriever instance and print out the results.
        QuotaRetriever qr = admin.getQuotaRetriever(qf);
        System.out.println("Quotas:");
        for (QuotaSettings setting : qr) {
            System.out.println("  Quota Setting: " + setting);
        }

        table.close();
        admin.close();
        helper.close();
    }
}