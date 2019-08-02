package coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import coprocessor.generated.RowCounterProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * RowCountEndpoint Example endpoint implementation, adding a row and cell count method.
 */
public class RowCountEndpoint extends RowCounterProtos.RowCountService implements Coprocessor {

    private RegionCoprocessorEnvironment env;

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
    public void getRowCount(RpcController controller,
                            RowCounterProtos.CountRequest request,
                            RpcCallback<RowCounterProtos.CountResponse> done) {
        RowCounterProtos.CountResponse response = null;
        try {
            long count = getCount(new FirstKeyOnlyFilter(), false);
            response = RowCounterProtos.CountResponse.newBuilder()
                    .setCount(count).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        }
        done.run(response);
    }

    @Override
    public void getCellCount(RpcController controller,
                             RowCounterProtos.CountRequest request,
                             RpcCallback<RowCounterProtos.CountResponse> done) {
        RowCounterProtos.CountResponse response = null;
        try {
            long count = getCount(null, true);
            response = RowCounterProtos.CountResponse.newBuilder()
                    .setCount(count).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        }
        done.run(response);
    }

    /**
     * HBaseUtils method to count rows or cells.
     * *
     *
     * @param filter     The optional filter instance.
     * @param countCells Hand in <code>true</code> for cell counting.
     * @return The count as per the flags.
     * @throws IOException When something fails with the scan.
     */
    private long getCount(Filter filter, boolean countCells)
            throws IOException {
        long count = 0;
        Scan scan = new Scan();
        scan.readVersions(1);
        if (filter != null) {
            scan.setFilter(filter);
        }
        try (InternalScanner scanner = env.getRegion().getScanner(scan)) {
            List<Cell> results = new ArrayList<>();
            boolean hasMore;
            byte[] lastRow = null;
            do {
                hasMore = scanner.next(results);
                for (Cell cell : results) {
                    if (!countCells) {
                        if (lastRow == null || !CellUtil.matchingRows(cell, lastRow)) {
                            lastRow = CellUtil.cloneRow(cell);
                            count++;
                        }
                    } else {
                        count++;
                    }
                }
                results.clear();
            } while (hasMore);
        }
        return count;
    }
}