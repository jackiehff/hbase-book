package coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import coprocessor.generated.ScanControlProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

/**
 * ScanControlObserverEndpoint Observer and endpoint for scan operations
 */
public class ScanControlObserverEndpoint extends ScanControlProtos.ScanControlService implements Coprocessor, RegionObserver {

    private RegionCoprocessorEnvironment env;
    private BitSet stopRows = new BitSet();

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
            Configuration conf = env.getConfiguration();
            String rows = conf.get("com.larsgeorge.copro.stoprows", "5");
            for (String row : rows.split(",")) {
                stopRows.set(Integer.parseInt(row));
            }
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        // nothing to do when coprocessor is shutting down
    }

    @Override
    public void resumeScan(RpcController controller,
                           ScanControlProtos.ScanControlRequest request,
                           RpcCallback<ScanControlProtos.ScanControlResponse> done) {
        ScanControlProtos.ScanControlResponse response = null;
        try {
            response = ScanControlProtos.ScanControlResponse.getDefaultInstance();
        } catch (Exception e) {
            ResponseConverter.setControllerException(controller, new IOException(e));
        }
        done.run(response);
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s,
                                  List<Result> result, int limit, boolean hasNext) {
        return false;
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s,
                                   List<Result> result, int limit, boolean hasNext) {
        return false;
    }

    @Override
    public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) {
        return false;
    }

    @Override
    public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) {
        return false;
    }

    @Override
    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c,
                                  byte[] row, byte[] family, byte[] qualifier,
                                  CompareOperator compareOp, ByteArrayComparable comparator, Put put,
                                  boolean result) {
        return false;
    }

    @Override
    public boolean preCheckAndPutAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
            byte[] qualifier, CompareOperator compareOp,
            ByteArrayComparable comparator, Put put, boolean result) {
        return false;
    }

    @Override
    public boolean postCheckAndPut(
            ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
            byte[] qualifier, CompareOperator compareOp,
            ByteArrayComparable comparator, Put put, boolean result) {
        return false;
    }

    @Override
    public boolean preCheckAndDelete(
            ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
            byte[] qualifier, CompareOperator compareOp,
            ByteArrayComparable comparator, Delete delete, boolean result) {
        return false;
    }

    @Override
    public boolean preCheckAndDeleteAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
            byte[] qualifier, CompareOperator compareOp,
            ByteArrayComparable comparator, Delete delete, boolean result) {
        return false;
    }

    @Override
    public boolean postCheckAndDelete(
            ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
            byte[] qualifier, CompareOperator compareOp,
            ByteArrayComparable comparator, Delete delete, boolean result) {
        return false;
    }

    @Override
    public StoreFileReader preStoreFileReaderOpen(
            ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p,
            FSDataInputStreamWrapper in, long size, CacheConfig cacheConf, Reference r,
            StoreFileReader reader) {
        return null;
    }

    @Override
    public StoreFileReader postStoreFileReaderOpen(
            ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p,
            FSDataInputStreamWrapper in, long size, CacheConfig cacheConf, Reference r,
            StoreFileReader reader) {
        return null;
    }

    @Override
    public Cell postMutationBeforeWAL(
            ObserverContext<RegionCoprocessorEnvironment> ctx, MutationType opType,
            Mutation mutation, Cell oldCell, Cell newCell) {
        return null;
    }

    @Override
    public DeleteTracker postInstantiateDeleteTracker(
            ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker) {
        return null;
    }
}
