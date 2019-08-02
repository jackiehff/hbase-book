package coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.Region;

/**
 * RegionServerObserverExample Example region server observer that ...
 */
public class RegionServerObserverExample implements RegionObserver {
    public static final Log LOG = LogFactory.getLog(Region.class);

   /* @Override
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c,
                          Region regionA, Region regionB, Region mergedRegion) throws IOException {
        RegionInfo regionInfo = mergedRegion.getRegionInfo();
//    mergedRegion.getWAL().append(mergedRegion.getTableDesc(), regionInfo,
//      new WALKey(mergedRegion.getRegionName(), regionInfo.getTable()),
//      new WALEdit().add(CellUtil.createCell()))
    }*/

}
