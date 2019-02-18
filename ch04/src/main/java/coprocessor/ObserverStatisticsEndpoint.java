package coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import coprocessor.generated.ObserverStatisticsProtos;
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
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ObserverStatisticsEndpoint Observer collecting invocation statistics.
 */
public class ObserverStatisticsEndpoint extends ObserverStatisticsProtos.ObserverStatisticsService implements Coprocessor, RegionObserver {

    private RegionCoprocessorEnvironment env;
    private Map<String, Integer> stats = new LinkedHashMap<>();

    // Lifecycle methods

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        // nothing to do when coprocessor is shutting down
    }

    @Override
    public void getStatistics(RpcController controller,
                              ObserverStatisticsProtos.StatisticsRequest request,
                              RpcCallback<ObserverStatisticsProtos.StatisticsResponse> done) {
        ObserverStatisticsProtos.StatisticsResponse response = null;
        try {
            ObserverStatisticsProtos.StatisticsResponse.Builder builder =
                    ObserverStatisticsProtos.StatisticsResponse.newBuilder();
            ObserverStatisticsProtos.NameInt32Pair.Builder pair =
                    ObserverStatisticsProtos.NameInt32Pair.newBuilder();
            for (Map.Entry<String, Integer> entry : stats.entrySet()) {
                pair.setName(entry.getKey());
                pair.setValue(entry.getValue().intValue());
                builder.addAttribute(pair.build());
            }
            response = builder.build();
            // optionally clear out stats
            if (request.hasClear() && request.getClear()) {
                synchronized (stats) {
                    stats.clear();
                }
            }
        } catch (Exception e) {
            ResponseConverter.setControllerException(controller,
                    new IOException(e));
        }
        done.run(response);
    }

    /**
     * Internal helper to keep track of call counts.
     *
     * @param call The name of the call.
     */
    private void addCallCount(String call) {
        synchronized (stats) {
            Integer count = stats.get(call);
            if (count == null) {
                count = new Integer(1);
            } else {
                count = new Integer(count + 1);
            }
            stats.put(call, count);
        }
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("preOpen");
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("postOpen");
    }

    /*@Override
    public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("postLogReplay");
    }

    @Override
    public InternalScanner preFlushScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            KeyValueScanner keyValueScanner, InternalScanner internalScanner) {
        addCallCount("preFlushScannerOpen");
        return internalScanner;
    }

    @Override
    public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
                                               KeyValueScanner keyValueScanner, InternalScanner internalScanner, long l) {
        return null;
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("preFlush1");
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, InternalScanner internalScanner) {
        addCallCount("preFlush2");
        return internalScanner;
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("postFlush1");
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, StoreFile storeFile) {
        addCallCount("postFlush2");
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, List<StoreFile> list, CompactionRequest compactionRequest) {
        addCallCount("preCompactSelection1");
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, List<StoreFile> list) {
        addCallCount("preCompactSelection2");
    }

    @Override
    public void postCompactSelection(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            ImmutableList<StoreFile> immutableList,
            CompactionRequest compactionRequest) {
        addCallCount("postCompactSelection1");
    }

    @Override
    public void postCompactSelection(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            ImmutableList<StoreFile> immutableList) {
        addCallCount("postCompactSelection2");
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
                                      InternalScanner internalScanner, ScanType scanType, CompactionRequest compactionRequest) {
        addCallCount("preCompact1");
        return internalScanner;
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
                                      InternalScanner internalScanner, ScanType scanType) {
        addCallCount("preCompact2");
        return internalScanner;
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
                                                 List<? extends KeyValueScanner> list, ScanType scanType, long l, InternalScanner internalScanner, CompactionRequest compactionRequest) {
        addCallCount("preCompactScannerOpen1");
        return internalScanner;
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, List<? extends KeyValueScanner> list, ScanType scanType, long l, InternalScanner internalScanner, CompactionRequest compactionRequest, long l1) {
        return null;
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
                                                 List<? extends KeyValueScanner> list, ScanType scanType, long l, InternalScanner internalScanner) {
        addCallCount("preCompactScannerOpen2");
        return internalScanner;
    }*/

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, StoreFile storeFile, CompactionLifeCycleTracker tracker, CompactionRequest compactionRequest) {
        addCallCount("postCompact1");
    }

    /*@Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, StoreFile storeFile) {
        addCallCount("postCompact2");
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("preSplit1");
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes) {
        addCallCount("preSplit2");
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext, Region region, Region region1) {
        addCallCount("postSplit");
    }

    @Override
    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, List<Mutation> list) {
        addCallCount("preSplitBeforePONR");
    }

    @Override
    public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("preSplitAfterPONR");
    }

    @Override
    public void preRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("preRollBackSplit");
    }

    @Override
    public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("postRollBackSplit");
    }

    @Override
    public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("postCompleteSplit");
    }*/

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> observerContext, boolean b) {
        addCallCount("preClose");
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> observerContext, boolean b) {
        addCallCount("postClose");
    }

    /*@Override
    public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
                                       byte[] bytes1, Result result) {
        addCallCount("preGetClosestRowBefore");
    }

    @Override
    public void postGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
                                        byte[] bytes1, Result result) {
        addCallCount("postGetClosestRowBefore");
    }*/

    @Override
    public void preGetOp(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get,
            List<Cell> list) {
        addCallCount("preGetOp");
    }

    @Override
    public void postGetOp(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get,
            List<Cell> list) {
        addCallCount("postGetOp");
    }

    @Override
    public boolean preExists(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get,
            boolean b) {
        addCallCount("preExists");
        return b;
    }

    @Override
    public boolean postExists(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get,
            boolean b) {
        addCallCount("postExists");
        return b;
    }

    @Override
    public void prePut(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put,
            WALEdit walEdit, Durability durability) {
        addCallCount("prePut");
    }

    @Override
    public void postPut(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put,
            WALEdit walEdit, Durability durability) {
        addCallCount("postPut");
    }

    @Override
    public void preDelete(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Delete delete, WALEdit walEdit, Durability durability) {
        addCallCount("preDelete");
    }

    /*@Override
    public void prePrepareTimeStampForDeleteVersion(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Mutation mutation, Cell cell, byte[] bytes, Get get) throws IOException {
        addCallCount("prePrepareTimeStampForDeleteVersion");
    }*/

    @Override
    public void postDelete(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Delete delete, WALEdit walEdit, Durability durability) {
        addCallCount("postDelete");
    }

    @Override
    public void preBatchMutate(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            MiniBatchOperationInProgress<Mutation> miniBatchOperationInProgress) {
        addCallCount("preBatchMutate");
    }

    @Override
    public void postBatchMutate(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            MiniBatchOperationInProgress<Mutation> miniBatchOperationInProgress) {
        addCallCount("postBatchMutate");
    }

    @Override
    public void postStartRegionOperation(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            HRegion.Operation operation) {
        addCallCount("postStartRegionOperation");
        addCallCount("- postStartRegionOperation-" + operation);
    }

    @Override
    public void postCloseRegionOperation(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            HRegion.Operation operation) {
        addCallCount("postCloseRegionOperation");
        addCallCount("- postCloseRegionOperation-" + operation);
    }

    @Override
    public void postBatchMutateIndispensably(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            MiniBatchOperationInProgress<Mutation> miniBatchOperationInProgress,
            boolean b) {
        addCallCount("postBatchMutateIndispensably");
    }

    @Override
    public boolean preCheckAndPut(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareOperator compareOp,
            ByteArrayComparable byteArrayComparable, Put put, boolean b) {
        addCallCount("preCheckAndPut");
        return b;
    }

    @Override
    public boolean preCheckAndPutAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareOperator compareOp,
            ByteArrayComparable byteArrayComparable, Put put, boolean b) {
        addCallCount("preCheckAndPutAfterRowLock");
        return b;
    }

    @Override
    public boolean postCheckAndPut(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareOperator compareOp,
            ByteArrayComparable byteArrayComparable, Put put, boolean b) {
        addCallCount("postCheckAndPut");
        return b;
    }

    @Override
    public boolean preCheckAndDelete(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareOperator compareOp,
            ByteArrayComparable byteArrayComparable, Delete delete, boolean b) {
        addCallCount("preCheckAndDelete");
        return b;
    }

    @Override
    public boolean preCheckAndDeleteAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareOperator compareOp,
            ByteArrayComparable byteArrayComparable, Delete delete, boolean b) {
        addCallCount("preCheckAndDeleteAfterRowLock");
        return b;
    }

    @Override
    public boolean postCheckAndDelete(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareOperator compareOp,
            ByteArrayComparable byteArrayComparable, Delete delete, boolean b) {
        addCallCount("postCheckAndDelete");
        return b;
    }

    /*@Override
    public long preIncrementColumnValue(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, long l, boolean b) {
        addCallCount("preIncrementColumnValue");
        return l;
    }

    @Override
    public long postIncrementColumnValue(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, long l, boolean b, long l1) {
        addCallCount("postIncrementColumnValue");
        return l;
    }*/

    @Override
    public Result preAppend(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Append append) {
        addCallCount("preAppend");
        return null;
    }

    @Override
    public Result preAppendAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Append append) {
        addCallCount("preAppendAfterRowLock");
        return null;
    }

    @Override
    public Result postAppend(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Append append, Result result) {
        addCallCount("postAppend");
        return result;
    }

    @Override
    public Result preIncrement(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Increment increment) {
        addCallCount("preIncrement");
        return null;
    }

    @Override
    public Result preIncrementAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Increment increment) {
        addCallCount("preIncrementAfterRowLock");
        return null;
    }

    @Override
    public Result postIncrement(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Increment increment, Result result) {
        addCallCount("postIncrement");
        return result;
    }

    /*@Override
    public RegionScanner preScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Scan scan,
            RegionScanner regionScanner) {
        addCallCount("preScannerOpen");
        return regionScanner;
    }

    @Override
    public KeyValueScanner preStoreScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            Scan scan, NavigableSet<byte[]> navigableSet,
            KeyValueScanner keyValueScanner) {
        addCallCount("preStoreScannerOpen");
        return keyValueScanner;
    }*/

    @Override
    public RegionScanner postScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Scan scan,
            RegionScanner regionScanner) {
        addCallCount("postScannerOpen");
        return regionScanner;
    }

    @Override
    public boolean preScannerNext(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner, List<Result> list, int i, boolean b) {
        addCallCount("preScannerNext");
        return b;
    }

    @Override
    public boolean postScannerNext(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner, List<Result> list, int i, boolean b) {
        addCallCount("postScannerNext");
        return b;
    }

    /*@Override
    public boolean postScannerFilterRow(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner, byte[] bytes, int i, short i1, boolean b) {
        addCallCount("postScannerFilterRow");
        return b;
    }*/

    @Override
    public void preScannerClose(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner) {
        addCallCount("preScannerClose");
    }

    @Override
    public void postScannerClose(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner) {
        addCallCount("postScannerClose");
    }

    /*@Override
    public void preWALRestore(
            ObserverContext<? extends RegionCoprocessorEnvironment> observerContext,
            RegionInfo regionInfo, WALKey walKey, WALEdit walEdit) {
        addCallCount("preWALRestore1");
    }

    @Override
    public void preWALRestore(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            RegionInfo hRegionInfo, WALKey hLogKey, WALEdit walEdit) {
        addCallCount("preWALRestore2");
    }

    @Override
    public void postWALRestore(
            ObserverContext<? extends RegionCoprocessorEnvironment> observerContext,
            RegionInfo hRegionInfo, WALKey walKey, WALEdit walEdit) {
        addCallCount("postWALRestore1");
    }

    @Override
    public void postWALRestore(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            RegionInfo hRegionInfo, WALKey hLogKey, WALEdit walEdit) {
        addCallCount("postWALRestore2");
    }*/

    @Override
    public void preBulkLoadHFile(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            List<Pair<byte[], String>> list) {
        addCallCount("preBulkLoadHFile");
    }

    /*@Override
    public boolean postBulkLoadHFile(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            List<Pair<byte[], String>> list, boolean b) {
        addCallCount("postBulkLoadHFile");
        return b;
    }*/

    @Override
    public StoreFileReader preStoreFileReaderOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            FileSystem fileSystem, Path path,
            FSDataInputStreamWrapper fsDataInputStreamWrapper, long l,
            CacheConfig cacheConfig, Reference reference, StoreFileReader reader) {
        addCallCount("preStoreFileReaderOpen");
        addCallCount("- preStoreFileReaderOpen-" + path.getName());
        return reader;
    }

    /*@Override
    public StoreFileReader postStoreFileReaderOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            FileSystem fileSystem, Path path,
            FSDataInputStreamWrapper fsDataInputStreamWrapper, long l,
            CacheConfig cacheConfig, Reference reference, HStoreFile.reader) {
        addCallCount("postStoreFileReaderOpen");
        addCallCount("- postStoreFileReaderOpen-" + path.getName());
        return reader;
    }*/

    @Override
    public Cell postMutationBeforeWAL(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            MutationType mutationType, Mutation mutation, Cell cell, Cell cell1) {
        addCallCount("postMutationBeforeWAL");
        addCallCount("- postMutationBeforeWAL-" + mutationType);
        return cell1;
    }

    @Override
    public DeleteTracker postInstantiateDeleteTracker(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            DeleteTracker deleteTracker) {
        addCallCount("postInstantiateDeleteTracker");
        return deleteTracker;
    }
}
