package admin;

import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * HColumnDescriptorExample Example how to create a HColumnDescriptor in code
 */
public class HColumnDescriptorExample {

    public static void main(String[] args) {
        ColumnFamilyDescriptor desc = ColumnFamilyDescriptorBuilder.newBuilder("colfam1".getBytes()).setValue("test-key", "test-value")
                .setBloomFilterType(BloomType.ROWCOL).build();

        System.out.println("Column Descriptor: " + desc);

        System.out.print("Values: ");
        for (Map.Entry<Bytes, Bytes> entry : desc.getValues().entrySet()) {
            System.out.print(Bytes.toString(entry.getKey().get()) +
                    " -> " + Bytes.toString(entry.getValue().get()) + ", ");
        }
        System.out.println();

        System.out.println("Defaults: " + ColumnFamilyDescriptorBuilder.getDefaultValues());

        System.out.println("Custom: " + desc.toStringCustomizedValues());

        System.out.println("Units:");
        System.out.println(ColumnFamilyDescriptorBuilder.TTL + " -> " + desc.getTimeToLive());
        System.out.println(ColumnFamilyDescriptorBuilder.BLOCKSIZE + " -> " + desc.getBlocksize());
    }
}
