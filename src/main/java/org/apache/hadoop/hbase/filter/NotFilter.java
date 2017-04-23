package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.*;

/**
 * org.apache.hadoop.hbase.filter.NotFilter
 * TODO implement a not filter?
 *
 * @author lhfcws
 * @since 2017/3/27
 */
public class NotFilter extends Filter {
    protected Filter filter;

    public NotFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public void reset() throws IOException {
        filter.reset();
    }

    @Override
    public boolean filterRowKey(byte[] bytes, int i, int i1) throws IOException {
        return !filter.filterRowKey(bytes, i, i1);
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
        return filter.filterAllRemaining();
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        ReturnCode code = this.filter.filterKeyValue(cell);

        if (code == ReturnCode.INCLUDE)
            code = ReturnCode.SKIP;
        else if (code == ReturnCode.INCLUDE_AND_NEXT_COL)
            code = ReturnCode.NEXT_COL;
        else if (code == ReturnCode.NEXT_ROW)
            code = ReturnCode.INCLUDE_AND_NEXT_COL;
        else if (code == ReturnCode.SKIP)
            code = ReturnCode.INCLUDE;
        else if (code == ReturnCode.NEXT_ROW)
            code = ReturnCode.INCLUDE;

        return code;
    }

    @Override
    public Cell transformCell(Cell cell) throws IOException {
        return filter.transformCell(cell);
    }

    @Override
    public KeyValue transform(KeyValue keyValue) throws IOException {
        return filter.transform(keyValue);
    }

    @Override
    public void filterRowCells(List<Cell> list) throws IOException {
        Set<Cell> l = new HashSet<>(list);
        filter.filterRowCells(list);
        l.removeAll(list);

        list.clear();
        list.addAll(l);
    }

    @Override
    public boolean hasFilterRow() {
        return filter.hasFilterRow();
    }

    @Override
    public boolean filterRow() throws IOException {
        return !filter.filterRow();
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue keyValue) throws IOException {
        return filter.getNextKeyHint(keyValue);
    }

    @Override
    public Cell getNextCellHint(Cell cell) throws IOException {
        return filter.getNextCellHint(cell);
    }

    @Override
    public boolean isFamilyEssential(byte[] bytes) throws IOException {
        return filter.isFamilyEssential(bytes);
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return filter.toByteArray();
    }

    boolean areSerializedFieldsEqual(Filter other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof NotFilter)) {
            return false;
        }
        NotFilter o = (NotFilter) other;
        return this.filter == o.filter;
    }

    @Override
    public String toString() {
        return "NotFilter (" +
                "Origin Filter: " + filter +
                ")";
    }
}
