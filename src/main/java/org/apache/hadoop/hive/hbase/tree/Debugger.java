package org.apache.hadoop.hive.hbase.tree;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.Arrays;

/**
 * @author lhfcws
 * @since 16/7/15
 */
public class Debugger {

    public static void print(SessionState.LogHelper log, String ... msg) {
        log.printInfo("[DEBUG] " + Arrays.toString(msg));
    }

    public static String toString(String parentExpr, ExprNodeDesc exprNodeDesc) {
        if (exprNodeDesc != null) {
            String udfname = TreeUtil.getGenericUDFNameFromExprDesc(exprNodeDesc);
            String s = "\nparentExpr: " + parentExpr + "\n"
                    + "name: " + exprNodeDesc.getName() + "\n"
                    + "udfname: " + udfname + "\n"
                    + "exprString: " + exprNodeDesc.getExprString() + "\n"
                    + "typeInfo: " + exprNodeDesc.getTypeInfo() + "\n"
                    + "typeString: " + exprNodeDesc.getTypeString() + "\n"
                    + "cols: " + exprNodeDesc.getCols() + "\n";
            return s;
        }
        return "";
    }

    public static String e2s(Throwable e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getMessage());
        for (StackTraceElement stackTraceElement : e.getStackTrace()) {
            sb.append("\n").append(stackTraceElement.toString());
        }
        return sb.toString();
    }

    public static void printExprNodeDesc(SessionState.LogHelper log, String parentExpr, ExprNodeDesc n) {
        if (n != null) {
            log.printInfo(toString(parentExpr, n));
            if (n.getChildren() != null)
                for (ExprNodeDesc desc : n.getChildren()) {
                    printExprNodeDesc( log, n.getExprString(), desc);
                }
        }
    }
}
