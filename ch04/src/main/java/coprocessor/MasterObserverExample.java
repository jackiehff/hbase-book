package coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterFileSystem;

import java.io.IOException;

/**
 * MasterObserverExample Example master observer that creates a separate directory on the file system when a table is created.
 */
public class MasterObserverExample implements MasterObserver {
    public static final Log LOG = LogFactory.getLog(MasterObserverExample.class);

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableDescriptor desc, RegionInfo[] regions) throws IOException {
        LOG.debug("Got postCreateTable callback");
        // co MasterObserverExample-1-GetName Get the new table's name from the table descriptor.
        TableName tableName = desc.getTableName();

        LOG.debug("Created table: " + tableName + ", region count: " + regions.length);

        // co MasterObserverExample-2-Services Retrieve a reference to the actual file system.
        MasterFileSystem masterFileSystem = new MasterFileSystem(ctx.getEnvironment().getConnection().getConfiguration());
        FileSystem fileSystem = masterFileSystem.getFileSystem();

        // co MasterObserverExample-3-Path Create a new directory that will store binary data from the client application.
        Path blobPath = new Path(tableName.getQualifierAsString() + "-blobs");
        fileSystem.mkdirs(blobPath);

        LOG.debug("Created " + blobPath + ": " + fileSystem.exists(blobPath));
    }
}
