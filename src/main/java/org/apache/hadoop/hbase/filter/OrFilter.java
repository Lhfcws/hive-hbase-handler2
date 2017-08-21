package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.List;

/**
 * org.apache.hadoop.hbase.filter.OrFilter
 *
 * @author xiangmin
 * @since 2017/4/18
 */
public class OrFilter extends Filter {
    private Filter f1;
    private Filter f2;

    private FilterList fl;

    public OrFilter(Filter f1, Filter f2) {
        this.f1 = f1;
        this.f2 = f2;
        fl = new FilterList(FilterList.Operator.MUST_PASS_ONE, f1, f2);
    }

    @Override
    public void reset() throws IOException {
        f1.reset();
        f2.reset();
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
        return fl.filterRowKey(buffer, offset, length);
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
        return fl.filterAllRemaining();
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        return fl.filterKeyValue(v);
    }

    @Override
    public Cell transformCell(Cell v) throws IOException {
        return fl.transformCell(v);
    }

    @Override
    public KeyValue transform(KeyValue currentKV) throws IOException {
        return fl.transform(currentKV);
    }

    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
        fl.filterRowCells(kvs);
    }

    @Override
    public boolean hasFilterRow() {
        return fl.hasFilterRow();
    }

    @Override
    public boolean filterRow() throws IOException {
        return fl.filterRow();
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue currentKV) throws IOException {
        return fl.getNextKeyHint(currentKV);
    }

    @Override
    public Cell getNextCellHint(Cell currentKV) throws IOException {
        return fl.getNextCellHint(currentKV);
    }

    @Override
    public boolean isFamilyEssential(byte[] name) throws IOException {
        return fl.isFamilyEssential(name);
    }

    @Override
    public byte[] toByteArray() throws IOException {
        // this may cause cause bug when perform parseFrom
        return fl.toByteArray();
    }

    @Override
    public String toString() {
        return "OrFilter: [" +
                f1 + ", " + f2 +
                "]";
    }

    @Override
    boolean areSerializedFieldsEqual(Filter other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof OrFilter)) {
            return false;
        }
        OrFilter o = (OrFilter) other;
        return this.f1 == o.f1 && this.f2 == o.f2;
    }
}
