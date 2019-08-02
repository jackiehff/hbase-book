package coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * RegionObserverWithBypassExample Example region observer checking for special get requests and bypassing further processing
 */
public class RegionObserverWithBypassExample implements RegionObserver {
    public static final Log LOG = LogFactory.getLog(HRegion.class);
    public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) {
        LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));
        if (Bytes.equals(get.getRow(), FIXED_ROW)) {
            long time = System.currentTimeMillis();
            // Create cell directly using the supplied utility.
            Cell cell = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                    .setRow(get.getRow())
                    .setFamily(FIXED_ROW)
                    .setQualifier(FIXED_ROW)
                    .setTimestamp(time)
                    .setType(KeyValue.Type.Put.getCode())
                    .setValue(Bytes.toBytes(time))
                    .build();
            LOG.debug("Had a match, adding fake cell: " + cell);
            results.add(cell);
            // Once the special cell is inserted all subsequent coprocessors are skipped.
            e.bypass();
        }
    }
}
