package htrace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.core.*;
import util.HBaseHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//import org.apache.htrace.Sampler;
//import org.apache.htrace.SamplerBuilder;
//import org.apache.htrace.Span;
//import org.apache.htrace.Trace;
//import org.apache.htrace.TraceScope;
//import org.apache.htrace.impl.ProbabilitySampler;

// cc HTraceExample Shows the use of the HBase HTrace integration
public class HTraceExample {

    // vv HTraceExample
    private static SpanReceiverHost spanReceiverHost;
    // ^^ HTraceExample

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 100, 100, "colfam1");

        // vv HTraceExample
        Map<String, String> configMap = new HashMap<>();
        configMap.put("hbase.trace.spanreceiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
        configMap.put("hbase.htrace.zipkin.collector-hostname", "localhost");
        configMap.put("hbase.htrace.zipkin.collector-port", "9410");

        conf.set("hbase.trace.spanreceiver.classes",
                "org.apache.htrace.impl.ZipkinSpanReceiver"); // co HTraceExample-1-Conf Set up configuration to use the Zipkin span receiver class.
        conf.set("hbase.htrace.zipkin.collector-hostname", "localhost");
        conf.set("hbase.htrace.zipkin.collector-port", "9410");

        spanReceiverHost = SpanReceiverHost.getInstance(conf); // co HTraceExample-2-GetInstance Initialize the span receiver host from the configuration settings.
        // ^^ HTraceExample

        Connection connection = null;

        Tracer tracer0 = new Tracer.Builder("Connection Trace").build();
        TraceScope ts0 = tracer0.newScope("Connection Trace");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } finally {
            ts0.close();
        }

        Admin admin = connection.getAdmin();
        admin.flush(TableName.valueOf("testtable"));
        Thread.sleep(3000);

        // vv HTraceExample
        Table table = connection.getTable(TableName.valueOf("testtable"));

        Tracer tracer1 = new Tracer.Builder("Get Trace").build();
        TraceScope ts1 = tracer1.newScope("Get Trace"); // co HTraceExample-2-Start Start a span, giving it a name and sample rate.
        try {
            Get get = new Get(Bytes.toBytes("row-1")); // co HTraceExample-3-Default Perform common operations that should be traced.
            Result res = table.get(get);
        } finally {
            ts1.close(); // co HTraceExample-4-Close Close the span to group performance details together.
        }
        //System.out.println("Is trace detached? " + ts1.isDetached()); // co HTraceExample-5-Span Talk to the trace and span instances from within the code.
        Span span = ts1.getSpan();
        System.out.println("Span Time: " + span.getAccumulatedMillis());
        System.out.println("Span: " + span);

        //conf.set("hbase.htrace.sampler", "ProbabilitySampler");
        //conf.set("hbase.htrace.sampler.fraction", "0.5");

        conf.set("hbase.htrace.sampler", "CountSampler");
        conf.set("hbase.htrace.sampler.frequency", "5");

        configMap.put("hbase.htrace.sampler", "CountSampler");
        configMap.put("hbase.htrace.sampler.frequency", "5");
        HTraceConfiguration traceConf = HTraceConfiguration.fromMap(configMap);
        Sampler.Builder builder = new Sampler.Builder(traceConf);
        Sampler sampler = builder.build();
        System.out.println("Sampler: " + sampler.getClass().getName());

        Tracer tracer2 = new Tracer.Builder("Scan Trace").build();
        TraceScope ts2 = tracer2.newScope("Scan Trace"); // co HTraceExample-6-Scan Start another span with a different sampler.
        try {
            Scan scan = new Scan();
            scan.setCaching(1); // co HTraceExample-7-OneRow The scan performs a separate RPC call for each row it retrieves, creating a span for every row.
            ResultScanner scanner = table.getScanner(scan);
            while (scanner.next() != null){}
            scanner.close();
        } finally {
            ts2.close();
        }
        // ^^ HTraceExample
        table.close();
        connection.close();
        admin.close();
    }
}
