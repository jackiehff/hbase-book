package mapreduce;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * BytesSplit Example for splitting a key range into pieces
 */
public class BytesSplit {

    public static void main(String[] args) {
        // co BytesSplit-1-9Splits The number defines the amount of splits performed. Splitting one region nine times results in ten parts.
        byte[][] splits = Bytes.split(Bytes.toBytes(0), Bytes.toBytes(100), 9);
        int n = 0;
        for (byte[] split : splits) {
            System.out.println("Split key[" + ++n + "]: " + Bytes.toInt(split));
        }
    }
}
