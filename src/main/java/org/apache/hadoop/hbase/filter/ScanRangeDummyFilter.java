package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * org.apache.hadoop.hbase.filter.ScanRangeDummyFilter
 * This is a dummy filter which means that it will not filter anything.
 *
 * @author lhfcws
 * @since 2017/3/28
 */
public class ScanRangeDummyFilter extends Filter {

    private byte[] startRow = null;
    private byte[] stopRow = null;

    public static class BytesComp extends Bytes.ByteArrayComparator {
        @Override
        public int compare(byte[] a, byte[] b) {
            if (a == null) {
                if (b == null)
                    return 0;
                else
                    return -1;
            } else {
                if (b == null)
                    return 1;
                else
                    return
                        super.compare(a, b);
            }
        }
    }

    public ScanRangeDummyFilter(byte[] startRow, byte[] stopRow) {
        this.startRow = startRow;
        this.stopRow = stopRow;
    }

    public void modifyScanRange(Scan scan) {
        if (startRow != null)
            scan.setStartRow(startRow);
        if (stopRow != null)
            scan.setStopRow(stopRow);
    }

    public boolean isFullScan() {
        return startRow == null && stopRow == null;
    }

    public byte[] getStartRow() {
        return startRow;
    }

    public byte[] getStopRow() {
        return stopRow;
    }

    public ScanRangeDummyFilter union(ScanRangeDummyFilter f) {
        int flag;
        BytesComp comp = new BytesComp();

        flag = comp.compare(this.startRow, f.startRow);
        if (flag < 0)
            this.startRow = f.startRow;

        flag = comp.compare(this.stopRow, f.stopRow);
        if (flag > 0 && f.stopRow != null)
            this.stopRow = f.stopRow;

        return this;
    }

    @Override
    public void reset() throws IOException {

    }

    @Override
    public boolean filterRowKey(byte[] bytes, int i, int i1) throws IOException {
        return false;
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
        return false;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        return ReturnCode.INCLUDE_AND_NEXT_COL;
    }

    @Override
    public Cell transformCell(Cell cell) throws IOException {
        return cell;
    }

    @Override
    public KeyValue transform(KeyValue keyValue) throws IOException {
        return keyValue;
    }

    @Override
    public void filterRowCells(List<Cell> list) throws IOException {

    }

    @Override
    public boolean hasFilterRow() {
        return false;
    }

    @Override
    public boolean filterRow() throws IOException {
        return false;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue keyValue) throws IOException {
        return keyValue;
    }

    @Override
    public Cell getNextCellHint(Cell cell) throws IOException {
        return cell;
    }

    @Override
    public boolean isFamilyEssential(byte[] bytes) throws IOException {
        return false;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return new byte[0];
    }

    @Override
    boolean areSerializedFieldsEqual(Filter filter) {
        return false;
    }

}
