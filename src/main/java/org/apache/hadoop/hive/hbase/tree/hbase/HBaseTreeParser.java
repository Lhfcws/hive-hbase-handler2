package org.apache.hadoop.hive.hbase.tree.hbase;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hive.hbase.tree.*;
import org.apache.hadoop.hive.hbase.tree.function.Function2;
import org.apache.hadoop.hive.hbase.tree.function.Pair;
import org.apache.hadoop.hive.hbase.tree.node.Node;
import org.apache.hadoop.hive.hbase.tree.node.OpNode;

import java.util.HashMap;
import java.util.List;

/**
 * org.apache.hadoop.hive.hbase.tree.hbase.HBaseTreeParser
 *
 * @author lhfcws
 * @since 2017/3/24
 */
public class HBaseTreeParser extends TreeParser<Filter> {
    static HashMap<String, CompareFilter.CompareOp> compareOpMap = new HashMap<>();

    static {
        compareOpMap.put("=", CompareFilter.CompareOp.EQUAL);
        compareOpMap.put("!=", CompareFilter.CompareOp.NOT_EQUAL);
        compareOpMap.put(">", CompareFilter.CompareOp.GREATER);
        compareOpMap.put(">=", CompareFilter.CompareOp.GREATER_OR_EQUAL);
        compareOpMap.put("<", CompareFilter.CompareOp.LESS);
        compareOpMap.put("<=", CompareFilter.CompareOp.LESS_OR_EQUAL);
    }


    HashMap<String, HBaseField> fieldMap = new HashMap<>();

    HBaseTreeParser() {
    }

    @Override
    public Filter parse(OpNode root) {
        if (root.isScanAllTable())
            return null;
        Filter filter = _parse(root);
        return filter;
    }

    protected Filter _parse(OpNode opNode) {
        String op = opNode.getOperator();
        if (TreeUtil.isRootOp(op)) {
            return parseRootNode(opNode);
        } else if (opNode.getSargableParser().isLogicOp(op)) {
            return parseLogicOp(opNode);
        } else if (opNode.getSargableParser().isSargableOp(op)) {
            return parseSargableOp(opNode);
        } else
            return null;
    }

    @Override
    public Filter parseRootNode(OpNode root) {
        Node wn = TreeUtil.safeget(root.getChildren(), 0);
        if (wn != null && wn instanceof OpNode) {
            Filter obj = _parse((OpNode) wn);
            if (obj != null) {
                return obj;
            } else
                return null;
        } else
            return null;
    }

    @Override
    public Filter parseLogicOp(OpNode logicOp) {
        String op = logicOp.getOperator();
        FilterList filterList = new FilterList();
        if ("or".equals(op)) {
            filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        }
        int i = 0;
        for (Node node : logicOp.getChildren()) {
            if (node instanceof OpNode) {
                OpNode n = (OpNode) node;
                Filter childRes = _parse(n);
                if (isEmpty(childRes)) continue;

                if ("and".equals(op)) {
                    makeFilter(filterList, childRes);
                } else if ("or".equals(op)) {
                    // pass
                    makeFilter(filterList, childRes);
                } else if ("not".equals(op)) {
//                    NotFilter notFilter = new NotFilter(childRes);
//                    return notFilter;
                    // pass
                    if (i++ <= 0) {
                        makeFilter(filterList, childRes);
                    } else {
                        makeFilter(filterList, new NotFilter(childRes));
                    }
                }
            }
        }
        if (isEmpty(filterList))
            return null;
        else
            return filterList;
    }

    @Override
    public Filter parseSargableOp(OpNode opNode) {
        final String op = opNode.getOperator();
        if (compareOpMap.containsKey(op)) {
            return TreeUtil.parseBinaryOp(opNode, new Function2<String, Object, Filter>() {
                @Override
                public Filter apply(String field, Object val) {
                    HBaseField hbField = fieldMap.get(field);
                    if (hbField == null)
                        return null;
                    if (!hbField.isRowKey())
                        return new SingleColumnValueFilter(
                                hbField.getColumnFamilyBytes(), hbField.getQualifierBytes(),
                                compareOpMap.get(op), String.valueOf(val).getBytes()
                        );
                    else {
                        // dont care if the boundary is included, because scan is default [,].
                        // and the data will be refiltered in hive.
                        if (op.equals(">") || op.equals(">=")) {
                            return new ScanRangeDummyFilter(
                                    String.valueOf(val).getBytes(), null
                            );
                        } else if (op.equals("<") || op.equals("<=")) {
                            return new ScanRangeDummyFilter(
                                    null, String.valueOf(val).getBytes()
                            );
                        } else if (op.equals("=")) {
                            byte[] valBytes = String.valueOf(val).getBytes();
                            return new ScanRangeDummyFilter(
                                    valBytes, valBytes
                            );
                        }
                        // ignore != op for rowkey, as it basically equals to full scan.
                    }
                    return null;
                }
            });
        } else if (op.equals("between")) {
            Pair<String, List<Object>> pair = TreeUtil.simpleExtractFieldNVals(opNode, "false");
            String field = pair.getFirst();
            HBaseField hbField = fieldMap.get(field);
            if (hbField == null)
                return null;

            Object val1 = TreeUtil.safeget(pair.getSecond(), 0);
            Object val2 = TreeUtil.safeget(pair.getSecond(), 1);
            if (TreeUtil.isAllNotNull(field, val1, val2)) {
                if (!hbField.isRowKey()) {
                    FilterList filterList = new FilterList();
                    SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                            hbField.getColumnFamilyBytes(), hbField.getQualifierBytes(),
                            CompareFilter.CompareOp.GREATER_OR_EQUAL, TreeUtil.toBytes(val1)
                    );
                    SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                            hbField.getColumnFamilyBytes(), hbField.getQualifierBytes(),
                            CompareFilter.CompareOp.LESS_OR_EQUAL, TreeUtil.toBytes(val2)
                    );
                    filterList.addFilter(filter1);
                    filterList.addFilter(filter2);
                    return filterList;
                } else {
                    ScanRangeDummyFilter dummyFilter = new ScanRangeDummyFilter(
                            String.valueOf(val1).getBytes(),
                            String.valueOf(val2).getBytes()
                    );
                    return dummyFilter;
                }
            }
        }

        return null;
    }

    private static boolean isEmpty(Filter filter) {
        return (filter == null) ||
                (filter instanceof FilterList) && ((FilterList) filter).getFilters().isEmpty();
    }

    private static Filter makeFilter(FilterList filterList, Filter toAdd) {
        if (toAdd == null)
            return filterList;
        if (toAdd instanceof FilterList) {
            for (Filter f : ((FilterList) toAdd).getFilters()) {
                filterList.addFilter(f);
            }
        } else {
            filterList.addFilter(toAdd);
        }
        return filterList;
    }

    private static Filter makeOrFilter(FilterList filterList, Filter toAdd) {
        if (toAdd == null)
            return filterList;
        if (toAdd instanceof FilterList) {
            for (Filter f : ((FilterList) toAdd).getFilters()) {
                filterList.addFilter(f);
            }
        } else {
            filterList.addFilter(toAdd);
        }

        return filterList;
    }

}
