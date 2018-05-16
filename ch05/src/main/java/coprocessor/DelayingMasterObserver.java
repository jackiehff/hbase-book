package coprocessor;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.AssignmentListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * DelayingMasterObserver Special master observer that delays region asignments
 */
public class DelayingMasterObserver extends BaseMasterObserver implements AssignmentListener {

    public static final Logger LOGGER = LoggerFactory.getLogger(DelayingMasterObserver.class);

    private Random rnd = new Random();

    @Override
    public void regionOpened(RegionInfo regionInfo, ServerName serverName) {
        try {
            if (regionInfo.getTable().getQualifierAsString().equals("testtable")) {
                long delay = rnd.nextInt(3);
                LOGGER.info("@@@ Delaying region " + regionInfo.getRegionNameAsString() +
                        " for " + delay + " seconds...");
                Thread.sleep(delay * 1000);
            }
        } catch (InterruptedException ie) {
            LOGGER.error(ie.getMessage());
        }
    }

    @Override
    public void regionClosed(RegionInfo hRegionInfo) {

    }

    @Override
    public void start(CoprocessorEnvironment ctx) {
        MasterCoprocessorEnvironment env = (MasterCoprocessorEnvironment) ctx;
        env.getMasterServices().getAssignmentManager().registerListener(this);
    }
}
// ^^ DelayingMasterObserver
