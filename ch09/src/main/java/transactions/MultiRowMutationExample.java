package transactions;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.List;

/**
 * MultiRowMutationExample Use the coprocessor based multi-row mutation call
 */
public class MultiRowMutationExample {

    public static void main(String[] args) throws IOException, InterruptedException, ServiceException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        TableName tableName = TableName.valueOf("testtable");

        // vv MultiRowMutationExample
        TableDescriptor htd = new TableDescriptorBuilder.ModifyableTableDescriptor(tableName)
                .setColumnFamily(new HColumnDescriptor("colfam1"))
                .setCoprocessor(MultiRowMutationEndpoint.class.getCanonicalName(), null, Coprocessor.PRIORITY_SYSTEM, null) // co MultiRowMutationExample-01-SetCopro Set the coprocessor explicitly for the table.
                .setValue(HTableDescriptor.SPLIT_POLICY, KeyPrefixRegionSplitPolicy.class.getName()) // co MultiRowMutationExample-02-SetSplitPolicy Set the supplied split policy.
                .setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, String.valueOf(2)); // co MultiRowMutationExample-03-SetPrefixLen Set the length of the prefix keeping entities together to two.

        System.out.println("Creating table...");
        Admin admin = connection.getAdmin();
        admin.createTable(htd);
        Table table = connection.getTable(tableName);

        // ^^ MultiRowMutationExample
        System.out.println("Filling table with test data...");
        // co MultiRowMutationExample-04-FillOne Fill first entity prefixed with two zeros, adding 10 rows.
        for (int i = 0; i < 10; i++) {
            Put put = new Put(Bytes.toBytes("00-row" + i));
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val1"));
            table.put(put);
        }

        // co MultiRowMutationExample-05-FillTwo Fill second entity prefixed with two nines, adding 10k rows.
        for (int i = 0; i < 10000; i++) {
            Put put = new Put(Bytes.toBytes("99-row" + i));
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("val1"));
            table.put(put);
        }

        // ^^ MultiRowMutationExample
        System.out.println("Flushing table...");
        // co MultiRowMutationExample-06-Flush Force a flush of the created data.
        admin.flush(tableName);
        Thread.sleep(3 * 1000L);

        List<RegionInfo> regions = admin.getRegions(tableName);
        int numRegions = regions.size();

        // ^^ MultiRowMutationExample
        System.out.println("Number of regions: " + numRegions);
        System.out.println("Splitting table...");
        // co MultiRowMutationExample-07-Split Subsequently split the table to test the split policy.
        admin.split(tableName);
        do {
            regions = admin.getRegions(tableName);
            Thread.sleep(1 * 1000L);
            System.out.print(".");
        } while (regions.size() <= numRegions);
        numRegions = regions.size();
        System.out.println("Number of regions: " + numRegions);
        System.out.println("Regions: ");
        // co MultiRowMutationExample-08-CheckBoundaries The region was split exactly between the two entities, despite the difference in size.
        for (RegionInfo info : regions) {
            System.out.print("  Start Key: " + Bytes.toString(info.getStartKey()));
            System.out.println(", End Key: " + Bytes.toString(info.getEndKey()));
        }

        MutateRowsRequest.Builder builder = MutateRowsRequest.newBuilder();

        Put put = new Put(Bytes.toBytes("00-row1"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val99999"));
        // co MultiRowMutationExample-09-AddPuts Add puts that address separate rows within the same entity (prefixed with two zeros).
        builder.addMutationRequest(ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, put));
        put = new Put(Bytes.toBytes("00-row5"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                Bytes.toBytes("val99999"));
        builder.addMutationRequest(ProtobufUtil.toMutation(
                ClientProtos.MutationProto.MutationType.PUT, put));

        // ^^ MultiRowMutationExample
        System.out.println("Calling mutation service...");
        // co MultiRowMutationExample-10-Endpoint Get the endpoint to the region that holds the proper entity (same prefix).
        CoprocessorRpcChannel channel = table.coprocessorService(Bytes.toBytes("00"));
        MultiRowMutationService.BlockingInterface service = MultiRowMutationService.newBlockingStub(channel);
        MutateRowsRequest request = builder.build();
        // co MultiRowMutationExample-11-Mutate Call the mutate method that updates the entity across multiple rows atomically.
        service.mutateRows(null, request);
        // ^^ MultiRowMutationExample

        System.out.println("Scanning first entity...");
        Scan scan = new Scan()
                .withStartRow(Bytes.toBytes("00"))
                .withStopRow(Bytes.toBytes("01"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.print("Result: " + result);
            byte[] val = result.getValue(Bytes.toBytes("colfam1"),
                    Bytes.toBytes("qual1"));
            System.out.println(", Value: " + Bytes.toString(val));
        }

        System.out.println(admin.getDescriptor(tableName));
        table.close();
        admin.close();
        connection.close();
    }
}
