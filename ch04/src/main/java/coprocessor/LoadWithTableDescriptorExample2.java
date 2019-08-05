package coprocessor;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;

/**
 * LoadWithTableDescriptorExample2 Load a coprocessor using the table descriptor using provided method
 */
public class LoadWithTableDescriptorExample2 {

    public static void main(String[] args) throws IOException {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);

        // Use fluent interface to create and configure the instance
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(HBaseConstants.TEST_TABLE)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colfam1")).build())
                .setCoprocessor(CoprocessorDescriptorBuilder.newBuilder(RegionObserverExample.class.getCanonicalName())
                        .setPriority(Coprocessor.PRIORITY_USER).build()).build();

        Admin admin = HBaseUtils.getConnection().getAdmin();
        admin.createTable(tableDescriptor);

        System.out.println(admin.getDescriptor(HBaseConstants.TEST_TABLE));
        admin.close();
        HBaseUtils.closeConnection();
    }
}
