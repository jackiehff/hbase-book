package coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;

import java.io.IOException;

/**
 * RegionServerObserverExample Example region server observer that ...
 */
public class RegionServerObserverExample implements MasterObserver {
    public static final Log LOG = LogFactory.getLog(RegionServerObserverExample.class);

    @Override
    public void postMergeRegionsCommitAction(ObserverContext<MasterCoprocessorEnvironment> ctx, RegionInfo[] regionsToMerge, RegionInfo mergedRegion) throws IOException {

    }

    /*@Override
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c,
                          Region regionA, Region regionB, Region mergedRegion) {
        RegionInfo regionInfo = mergedRegion.getRegionInfo();
//    mergedRegion.getWAL().append(mergedRegion.getTableDesc(), regionInfo,
//      new WALKey(mergedRegion.getRegionName(), regionInfo.getTable()),
//      new WALEdit().add(CellUtil.createCell()))
    }
*/
}
