package client;

import constant.HBaseConstants;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * BufferedMutatorExample Shows the use of the client side write buffer
 */
public class BufferedMutatorExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedMutatorExample.class);

    private static final int POOL_SIZE = 10;
    private static final int TASK_COUNT = 100;
    private static final byte[] FAMILY = Bytes.toBytes("colfam1");

    public static void main(String[] args) throws Exception {
        HBaseUtils.dropTable(HBaseConstants.TEST_TABLE);
        HBaseUtils.createTable(HBaseConstants.TEST_TABLE, "colfam1");

        // co BufferedMutatorExample-01-Listener Create a custom listener instance.
        BufferedMutator.ExceptionListener listener =
                (e, mutator) -> {
                    // co BufferedMutatorExample-02-OnException Handle callback in case of an exception.
                    for (int i = 0; i < e.getNumExceptions(); i++) {
                        // co BufferedMutatorExample-03-PrintRow Generically retrieve the mutation that failed, using the common superclass.
                        LOGGER.info("Failed to send put: " + e.getRow(i));
                    }
                };

        // co BufferedMutatorExample-04-Params Create a parameter instance, set the table name and custom listener reference.
        BufferedMutatorParams params = new BufferedMutatorParams(HBaseConstants.TEST_TABLE).listener(listener);

        try (BufferedMutator mutator = HBaseUtils.getConnection().getBufferedMutator(params)) {
            // co BufferedMutatorExample-06-Pool Create a worker pool to update the shared mutator in parallel.
            ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
            List<Future<Void>> futures = new ArrayList<>(TASK_COUNT);

            // co BufferedMutatorExample-07-Threads Start all the workers up.
            for (int i = 0; i < TASK_COUNT; i++) {
                futures.add(workerPool.submit(() -> {
                    Put p = new Put(Bytes.toBytes("row1"));
                    p.addColumn(FAMILY, Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
                    // co BufferedMutatorExample-08-Put Each worker uses the shared mutator instance, sharing the same backing buffer, callback listener, and RPC execuor pool.
                    mutator.mutate(p);
                    // [...]
                    // Do work... Maybe call mutator.flush() after many edits to ensure
                    // any of this worker's edits are sent before exiting the Callable
                    return null;
                }));
            }

            for (Future<Void> f : futures) {
                // co BufferedMutatorExample-09-Shutdown Wait for workers and shut down the pool.
                f.get(5, TimeUnit.MINUTES);
            }
            workerPool.shutdown();
        } catch (IOException e) { // co BufferedMutatorExample-10-ImplicitClose The try-with-resource construct ensures that first the mutator, and then the connection are closed. This could trigger exceptions and call the custom listener.
            LOGGER.error("Exception while creating or freeing resources", e);
        }
    }
}