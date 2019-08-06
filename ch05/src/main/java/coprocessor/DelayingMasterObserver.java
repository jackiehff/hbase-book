package coprocessor;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * DelayingMasterObserver Special master observer that delays region assignments
 */
public class DelayingMasterObserver implements MasterObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelayingMasterObserver.class);

    private Random rnd = new Random();

    @Override
    public void regionOpened(RegionInfo regionInfo, ServerName serverName) {
        try {
            if (StringUtils.equals("testtable", regionInfo.getTable().getQualifierAsString())) {
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
        try {
            MasterServices masterServices = new HMaster(ctx.getConfiguration());
            masterServices.getAssignmentManager().registerListener(this);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
