package filters;

// cc CustomFilter Implements a filter that lets certain rows pass

import com.google.protobuf.InvalidProtocolBufferException;
import filters.generated.FilterProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteStringer;

/**
 * Implements a custom filter for HBase. It takes a value and compares
 * it with every value in each KeyValue checked. Once there is a match
 * the entire row is passed, otherwise filtered out.
 */
public class CustomFilter extends FilterBase {

    private byte[] value = null;
    private boolean filterRow = true;

    public CustomFilter() {
        super();
    }

    public CustomFilter(byte[] value) {
        // Set the value to compare against.
        this.value = value;
    }

    @Override
    public void reset() {
        // Reset filter flag for each new row being tested.
        this.filterRow = true;
    }

    @Override
    public ReturnCode filterCell(Cell cell) {
        if (CellUtil.matchingValue(cell, value)) {
            // When there is a matching value, then let the row pass.
            filterRow = false;
        }
        // Always include, since the final decision is made later.
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRow() {
        // Here the actual decision is taking place, based on the flag status.
        return filterRow;
    }

    @Override
    public byte[] toByteArray() {
        FilterProtos.CustomFilter.Builder builder = FilterProtos.CustomFilter.newBuilder();
        if (value != null) {
            // Writes the given value out so it can be sent to the servers.
            builder.setValue(ByteStringer.wrap(value));
        }
        return builder.build().toByteArray();
    }

    public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
        FilterProtos.CustomFilter proto;
        try {
            // Used by the servers to establish the filter instance with the correct values.
            proto = FilterProtos.CustomFilter.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new CustomFilter(proto.getValue().toByteArray());
    }
}