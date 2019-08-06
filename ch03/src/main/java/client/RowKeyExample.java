package client;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * RowKeyExample Example row key usage from existing array
 */
public class RowKeyExample {

    public static void main(String[] args) {
        byte[] data = new byte[100];
        Arrays.fill(data, (byte) '@');
        String username = "johndoe";
        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);

        System.arraycopy(usernameBytes, 0, data, 45, usernameBytes.length);
        System.out.println("data length: " + data.length + ", data: " + Bytes.toString(data));

        Put put = new Put(data, 45, usernameBytes.length);
        System.out.println("Put: " + put);
    }
}
