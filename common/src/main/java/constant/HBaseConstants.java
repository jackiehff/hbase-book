package constant;

import org.apache.hadoop.hbase.TableName;

public interface HBaseConstants {

    public static final String TEST_TABLE_NAME = "testtable";
    public static final TableName TEST_TABLE = TableName.valueOf(TEST_TABLE_NAME);
}
