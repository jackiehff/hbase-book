package coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;

import java.io.IOException;

/**
 * MasterObserverExample Example master observer that creates a separate directory on the file system when a table is created.
 */
public class MasterObserverExample extends BaseMasterObserver {
    public static final Log LOG = LogFactory.getLog(MasterObserverExample.class);
    // vv MasterObserverExample

    @Override
    public void postCreateTable(
            ObserverContext<MasterCoprocessorEnvironment> ctx,
            TableDescriptor desc, RegionInfo[] regions)
            throws IOException {
        LOG.debug("Got postCreateTable callback");
        TableName tableName = desc.getTableName(); // co MasterObserverExample-1-GetName Get the new table's name from the table descriptor.

        LOG.debug("Created table: " + tableName + ", region count: " + regions.length);
        MasterServices services = ctx.getEnvironment().getMasterServices();
        MasterFileSystem masterFileSystem = services.getMasterFileSystem(); // co MasterObserverExample-2-Services Get the available services and retrieve a reference to the actual file system.
        FileSystem fileSystem = masterFileSystem.getFileSystem();

        Path blobPath = new Path(tableName.getQualifierAsString() + "-blobs"); // co MasterObserverExample-3-Path Create a new directory that will store binary data from the client application.
        fileSystem.mkdirs(blobPath);

        LOG.debug("Created " + blobPath + ": " + fileSystem.exists(blobPath));
    }
}
