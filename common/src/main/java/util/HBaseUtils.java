package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Used by the book examples to generate tables and fill them with test data.
 */
public class HBaseUtils {

    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        return connection;
    }

    public static Configuration getConfiguration() {
        return configuration;
    }

    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建命名空间
     *
     * @param namespace 命名空间
     */
    public static void createNamespace(String namespace) {
        try {
            NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nd);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * 删除命名空间
     *
     * @param namespace 命名空间
     * @param force     是否强制删除(true 会删除 namespace 下所有表)
     */
    public static void dropNamespace(String namespace, boolean force) {
        try {
            if (force) {
                TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
                for (TableName name : tableNames) {
                    admin.disableTable(name);
                    admin.deleteTable(name);
                }
            }
        } catch (Exception e) {
            // ignore
        }
        try {
            admin.deleteNamespace(namespace);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public static boolean existsTable(String table) throws IOException {
        return existsTable(TableName.valueOf(table));
    }

    public static boolean existsTable(TableName table) throws IOException {
        return admin.tableExists(table);
    }

    public static void createTable(String table, String... colfams) throws IOException {
        createTable(TableName.valueOf(table), 1, null, colfams);
    }

    public static void createTable(TableName table, String... colfams) throws IOException {
        createTable(table, 1, null, colfams);
    }

    public static void createTable(String table, int maxVersions, String... colfams) throws IOException {
        createTable(TableName.valueOf(table), maxVersions, null, colfams);
    }

    public static void createTable(TableName table, int maxVersions, String... colfams) throws IOException {
        createTable(table, maxVersions, null, colfams);
    }

    public static void createTable(String table, byte[][] splitKeys, String... colfams) throws IOException {
        createTable(TableName.valueOf(table), 1, splitKeys, colfams);
    }

    public static void createTable(TableName table, int maxVersions, byte[][] splitKeys, String... colfams) throws IOException {
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(table).build();
        TableDescriptorBuilder.ModifyableTableDescriptor desc = (TableDescriptorBuilder.ModifyableTableDescriptor) TableDescriptorBuilder.copy(tableDescriptor);
        for (String cf : colfams) {
            ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor coldef = (ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor) ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build();
            coldef.setMaxVersions(maxVersions);
            desc.setColumnFamily(coldef);
        }
        if (splitKeys != null) {
            admin.createTable(desc, splitKeys);
        } else {
            admin.createTable(desc);
        }
    }

    public static Table getTable(String table) throws IOException {
        return getTable(TableName.valueOf(table));
    }

    public static Table getTable(TableName table) throws IOException {
        return connection.getTable(table);
    }

    public static void disableTable(String table) throws IOException {
        disableTable(TableName.valueOf(table));
    }

    public static void disableTable(TableName table) throws IOException {
        admin.disableTable(table);
    }

    public static void dropTable(String table) throws IOException {
        dropTable(TableName.valueOf(table));
    }

    public static void dropTable(TableName table) throws IOException {
        if (existsTable(table)) {
            if (admin.isTableEnabled(table)) {
                disableTable(table);
            }
            admin.deleteTable(table);
        }
    }

    public static void fillTable(String table, int startRow, int endRow, int numCols, String... colfams) throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, colfams);
    }

    public static void fillTable(TableName table, int startRow, int endRow, int numCols, String... colfams) throws IOException {
        fillTable(table, startRow, endRow, numCols, -1, false, colfams);
    }

    public static void fillTable(String table, int startRow, int endRow, int numCols,
                                 boolean setTimestamp, String... colfams) throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, -1, setTimestamp, colfams);
    }

    public static void fillTable(TableName table, int startRow, int endRow, int numCols,
                                 boolean setTimestamp, String... colfams) throws IOException {
        fillTable(table, startRow, endRow, numCols, -1, setTimestamp, colfams);
    }

    public static void fillTable(String table, int startRow, int endRow, int numCols,
                                 int pad, boolean setTimestamp, String... colfams) throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, pad,
                setTimestamp, false, colfams);
    }

    public static void fillTable(TableName table, int startRow, int endRow, int numCols,
                                 int pad, boolean setTimestamp, String... colfams) throws IOException {
        fillTable(table, startRow, endRow, numCols, pad, setTimestamp, false, colfams);
    }

    public static void fillTable(String table, int startRow, int endRow, int numCols,
                                 int pad, boolean setTimestamp, boolean random, String... colfams)
            throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, pad,
                setTimestamp, random, colfams);
    }

    public static void fillTable(TableName table, int startRow, int endRow, int numCols,
                                 int pad, boolean setTimestamp, boolean random,
                                 String... colfams) throws IOException {
        try (Table tbl = connection.getTable(table)) {
            Random rnd = new Random();
            for (int row = startRow; row <= endRow; row++) {
                for (int col = 1; col <= numCols; col++) {
                    Put put = new Put(Bytes.toBytes("row-" + padNum(row, pad)));
                    for (String cf : colfams) {
                        String colName = "col-" + padNum(col, pad);
                        String val = "val-" + (random ?
                                Integer.toString(rnd.nextInt(numCols)) :
                                padNum(row, pad) + "." + padNum(col, pad));
                        if (setTimestamp) {
                            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), col, Bytes.toBytes(val));
                        } else {
                            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), Bytes.toBytes(val));
                        }
                    }
                    tbl.put(put);
                }
            }
        }
    }

    public static void fillTableRandom(String table,
                                       int minRow, int maxRow, int padRow,
                                       int minCol, int maxCol, int padCol,
                                       int minVal, int maxVal, int padVal,
                                       boolean setTimestamp, String... colfams)
            throws IOException {
        fillTableRandom(TableName.valueOf(table), minRow, maxRow, padRow, minCol,
                maxCol, padCol, minVal, maxVal, padVal, setTimestamp, colfams);
    }

    public static void fillTableRandom(TableName table,
                                       int minRow, int maxRow, int padRow,
                                       int minCol, int maxCol, int padCol,
                                       int minVal, int maxVal, int padVal,
                                       boolean setTimestamp, String... colfams) throws IOException {
        try (Table tbl = connection.getTable(table)) {
            Random rnd = new Random();
            int maxRows = minRow + rnd.nextInt(maxRow - minRow);
            for (int row = 0; row < maxRows; row++) {
                int maxCols = minCol + rnd.nextInt(maxCol - minCol);
                for (int col = 0; col < maxCols; col++) {
                    int rowNum = rnd.nextInt(maxRow - minRow + 1);
                    Put put = new Put(Bytes.toBytes("row-" + padNum(rowNum, padRow)));
                    for (String cf : colfams) {
                        int colNum = rnd.nextInt(maxCol - minCol + 1);
                        String colName = "col-" + padNum(colNum, padCol);
                        int valNum = rnd.nextInt(maxVal - minVal + 1);
                        String val = "val-" + padNum(valNum, padCol);
                        if (setTimestamp) {
                            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), col,
                                    Bytes.toBytes(val));
                        } else {
                            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName),
                                    Bytes.toBytes(val));
                        }
                    }
                    tbl.put(put);
                }
            }
        }
    }

    public static String padNum(int num, int pad) {
        String res = Integer.toString(num);
        if (pad > 0) {
            while (res.length() < pad) {
                res = "0" + res;
            }
        }
        return res;
    }

    public static void put(String table, String row, String fam, String qual, String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, val);
    }

    public static void put(TableName table, String row, String fam, String qual, String val) throws IOException {
        try (Table tbl = connection.getTable(table)) {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), Bytes.toBytes(val));
            tbl.put(put);
        }
    }

    public static void put(String table, String row, String fam, String qual, long ts,
                           String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, ts, val);
    }

    public static void put(TableName table, String row, String fam, String qual, long ts,
                           String val) throws IOException {
        try (Table tbl = connection.getTable(table)) {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), ts,
                    Bytes.toBytes(val));
            tbl.put(put);
        }
    }

    public static void put(String table, String[] rows, String[] fams, String[] quals,
                           long[] ts, String[] vals) throws IOException {
        put(TableName.valueOf(table), rows, fams, quals, ts, vals);
    }

    public static void put(TableName table, String[] rows, String[] fams, String[] quals,
                           long[] ts, String[] vals) throws IOException {
        try (Table tbl = connection.getTable(table)) {
            for (String row : rows) {
                Put put = new Put(Bytes.toBytes(row));
                for (String fam : fams) {
                    int v = 0;
                    for (String qual : quals) {
                        String val = vals[v < vals.length ? v : vals.length - 1];
                        long t = ts[v < ts.length ? v : ts.length - 1];
                        System.out.println("Adding: " + row + " " + fam + " " + qual +
                                " " + t + " " + val);
                        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), t,
                                Bytes.toBytes(val));
                        v++;
                    }
                }
                tbl.put(put);
            }
        }
    }

    public static void dump(String table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        dump(TableName.valueOf(table), rows, fams, quals);
    }

    public static void dump(TableName table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        try (Table tbl = connection.getTable(table)) {
            List<Get> gets = new ArrayList<>();
            for (String row : rows) {
                Get get = new Get(Bytes.toBytes(row));
                get.readAllVersions();
                if (fams != null) {
                    for (String fam : fams) {
                        for (String qual : quals) {
                            get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
                        }
                    }
                }
                gets.add(get);
            }
            Result[] results = tbl.get(gets);
            for (Result result : results) {
                dumpResult(result);
            }
        }
    }

    public static void dump(String table) throws IOException {
        dump(TableName.valueOf(table));
    }

    public static void dump(TableName table) throws IOException {
        try (
                Table t = connection.getTable(table);
                ResultScanner scanner = t.getScanner(new Scan())
        ) {
            for (Result result : scanner) {
                dumpResult(result);
            }
        }
    }

    public static void dumpResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell +
                    ", Value: " + Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength()));
        }
    }
}
