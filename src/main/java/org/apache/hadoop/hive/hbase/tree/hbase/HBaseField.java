package org.apache.hadoop.hive.hbase.tree.hbase;


import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

/**
 * org.apache.hadoop.hive.hbase.tree.hbase.HBaseField
 *
 * @author lhfcws
 * @since 2017/3/27
 */
public class HBaseField implements Serializable {
    private String columnFamily = null;
    private String qualifier = null;
    private boolean isRowKey = false;

    public HBaseField(String rowKey) {
        this.columnFamily = rowKey;
        this.isRowKey = true;
    }

    public HBaseField(String columnFamily, String qualifier) {
        this.columnFamily = columnFamily;
        this.qualifier = qualifier;
    }

    public boolean isRowKey() {
        return isRowKey;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getQualifier() {
        return qualifier;
    }

    public byte[] getColumnFamilyBytes() {
        if (!isRowKey() ) {
            if (columnFamily == null)
                return null;
            else
                return Bytes.toBytes(columnFamily);
        } else
            return null;
    }

    public byte[] getQualifierBytes() {
        if (!isRowKey() ) {
            if (qualifier == null)
                return null;
            else
                return Bytes.toBytes(qualifier);
        } else
            return null;
    }

    public String getRowKey() {
        if (isRowKey())
            return columnFamily;
        else
            return null;
    }

    public byte[] getRowKeyBytes() {
        if (isRowKey())
            return Bytes.toBytes(columnFamily);
        else
            return null;
    }
}
