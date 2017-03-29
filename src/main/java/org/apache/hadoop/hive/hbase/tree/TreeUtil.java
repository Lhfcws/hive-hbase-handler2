package org.apache.hadoop.hive.hbase.tree;

import org.apache.hadoop.hive.hbase.tree.function.Function2;
import org.apache.hadoop.hive.hbase.tree.function.Pair;
import org.apache.hadoop.hive.hbase.tree.node.ConstantNode;
import org.apache.hadoop.hive.hbase.tree.node.FieldNode;
import org.apache.hadoop.hive.hbase.tree.node.Node;
import org.apache.hadoop.hive.hbase.tree.node.OpNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.*;

/**
 * org.apache.hadoop.hive.hbase.tree.TreeUtil
 *
 * @author lhfcws
 * @since 2017/3/24
 */
public class TreeUtil {
    public static final String ROOT_NAME = "ROOT";

    public static boolean isRootOp(String op) {
        return op != null && "ROOT".equals(op);
    }

    public static boolean isLeafNode(Node wn) {
        return !wn.hasChildren();
    }

    public static String stripStrVal(String val) {
        if (val.startsWith("'") && val.endsWith("'"))
            val = val.substring(1, val.length() - 1);
        return val.toLowerCase();
    }

    public static void printNThrowErr(SessionState.LogHelper console, String msg) throws Exception {
        console.printError(msg);
        throw new Exception(msg);
    }

    public static String getGenericUDFNameFromExprDesc(ExprNodeDesc desc) {
        if (!(desc instanceof ExprNodeGenericFuncDesc)) {
            return null;
        } else {
            ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) desc;
            return genericFuncDesc.getGenericUDF().getUdfName();
        }
    }

    public static Pair<String, List<Object>> simpleExtractFieldNVals(OpNode opNode, Object... ignoreVals) {
        Set<Object> ignore = new HashSet<>();
        if (ignoreVals != null && ignoreVals.length > 0)
            ignore.addAll(Arrays.asList(ignoreVals));

        String field = null;
        List<Object> vals = new ArrayList<>();
        for (Node node : opNode.getChildren()) {
            if (node instanceof FieldNode) {
                field = ((FieldNode) node).getField();
            } else if (node instanceof ConstantNode) {
                ConstantNode constantNode = (ConstantNode) node;
                Object v = constantNode.getValue();
                if ("string".equals(constantNode.getValueType()))
                    v = TreeUtil.stripStrVal(v.toString());

                if (ignore.contains(v))
                    continue;
                vals.add(v);
            }
        }

        return new Pair<>(field, vals);
    }

    public static <T> T safeget(List<T> list, int index) {
        if (list != null && list.size() > index)
            try {
                return list.get(index);
            } catch (Exception e) {
                return null;
            }
        else
            return null;
    }

    public static boolean isAllNotNull(Object ... objs) {
        for (Object o : objs) {
            if (o == null)
                return false;
        }
        return true;
    }

    public static <T> T parseBinaryOp(OpNode opNode, Function2<String, Object, T> func) {
        Pair<String, List<Object>> pair = TreeUtil.simpleExtractFieldNVals(opNode);
        String field = pair.getFirst();
        Object val = TreeUtil.safeget(pair.getSecond(), 0);
        if (TreeUtil.isAllNotNull(field, val)) {
            return func.apply(field, val);
        } else
            return null;
    }

    public static byte[] toBytes(Object o) {
        if (o == null)
            return null;
        else
            return String.valueOf(o).getBytes();
    }
}
