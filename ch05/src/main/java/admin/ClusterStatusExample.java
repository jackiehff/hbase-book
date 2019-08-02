package admin;

import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * ClusterStatusExample Example reporting the status of a cluster
 */
public class ClusterStatusExample {

    public static void main(String[] args) throws IOException {
        Admin admin = HBaseUtils.getConnection().getAdmin();

        // ClusterStatusExample-1-GetStatus Get the cluster status.
        ClusterMetrics status = admin.getClusterMetrics();

        System.out.println("Cluster Status:\n--------------");
        System.out.println("HBase Version: " + status.getHBaseVersion());
        System.out.println("Version: " + status.getHBaseVersion());
        System.out.println("Cluster ID: " + status.getClusterId());
        System.out.println("Master: " + status.getMasterName());
        System.out.println("No. Backup Masters: " + status.getBackupMasterNames().size());
        System.out.println("Backup Masters: " + status.getBackupMasterNames());

        System.out.println("No. Live Servers: " + status.getLiveServerMetrics().size());
        System.out.println("Servers: " + status.getLiveServerMetrics());
        System.out.println("No. Dead Servers: " + status.getDeadServerNames());
        System.out.println("Dead Servers: " + status.getDeadServerNames());
        System.out.println("No. Regions: " + status.getRegionCount());
        System.out.println("Regions in Transition: " + status.getRegionStatesInTransition());
        System.out.println("No. Requests: " + status.getRequestCount());
        System.out.println("Avg Load: " + status.getAverageLoad());
        System.out.println("Balancer On: " + status.getBalancerOn());
        System.out.println("Is Balancer On: " + status.getBalancerOn());
        System.out.println("Master Coprocessors: " + status.getMasterCoprocessorNames());

        System.out.println("\nServer Info:\n--------------");
        // co ClusterStatusExample-2-ServerInfo Iterate over the included server instances.

        for (Map.Entry<ServerName, ServerMetrics> serverEntry : status.getLiveServerMetrics().entrySet()) {
            ServerName server = serverEntry.getKey();
            System.out.println("Hostname: " + server.getHostname());
            System.out.println("Host and Port: " + server.getAddress().toString());
            System.out.println("Server Name: " + server.getServerName());
            System.out.println("RPC Port: " + server.getPort());
            System.out.println("Start Code: " + server.getStartcode());

            // co ClusterStatusExample-3-ServerLoad Retrieve the load details for the current server.
            ServerMetrics load = serverEntry.getValue();
            //ClusterStatusProtos.ServerLoad

            System.out.println("\nServer Load:\n--------------");
            System.out.println("Info Port: " + load.getInfoServerPort());
            System.out.println("Max Heap (MB): " + load.getMaxHeapSize());
            System.out.println("Used Heap (MB): " + load.getUsedHeapSize());
            System.out.println("No. Regions: " + load.getRegionMetrics().size());
            System.out.println("No. Requests: " + load.getRequestCount());
            System.out.println("Total No. Requests: " + load.getRequestCount());
            System.out.println("No. Requests per Sec: " + load.getRequestCountPerSecond());
            System.out.println("Coprocessors: " + Arrays.asList(load.getCoprocessorNames()));
            System.out.println("Replication Load Sink: " + load.getReplicationLoadSink());
            System.out.println("Replication Load Source: " + load.getReplicationLoadSourceList());

            System.out.println("\nRegion Load:\n--------------");
            // co ClusterStatusExample-4-Regions Iterate over the region details of the current server.
            for (Map.Entry<byte[], RegionMetrics> regionEntry : load.getRegionMetrics().entrySet()) {
                System.out.println("Region: " + Bytes.toStringBinary(regionEntry.getKey()));

                // co ClusterStatusExample-5-RegionLoad Get the load details for the current region.
                RegionMetrics regionLoad = regionEntry.getValue();

                System.out.println("Name: " + Bytes.toStringBinary(regionLoad.getRegionName()));
                System.out.println("Name (as String): " + regionLoad.getNameAsString());
                System.out.println("No. Requests: " + regionLoad.getRequestCount());
                System.out.println("No. Read Requests: " + regionLoad.getReadRequestCount());
                System.out.println("No. Write Requests: " + regionLoad.getWriteRequestCount());
                System.out.println("No. Stores: " + regionLoad.getStoreCount());
                System.out.println("No. Storefiles: " + regionLoad.getStoreFileCount());
                System.out.println("Data Locality: " + regionLoad.getDataLocality());
                System.out.println("Storefile Size (MB): " + regionLoad.getStoreFileSize());
                System.out.println("Storefile Index Size (MB): " + regionLoad.getStoreFileIndexSize());
                System.out.println("Memstore Size (MB): " + regionLoad.getMemStoreSize());
                System.out.println("Root Index Size: " + regionLoad.getStoreFileRootLevelIndexSize());
                System.out.println("Total Bloom Size: " + regionLoad.getBloomFilterSize());
                System.out.println("Total Index Size: " + regionLoad.getStoreFileUncompressedDataIndexSize());
                System.out.println("Current Compacted Cells: " + regionLoad.getCompactedCellCount());
                System.out.println("Total Compacting Cells: " + regionLoad.getCompactingCellCount());
                System.out.println();
            }
        }
    }
}
