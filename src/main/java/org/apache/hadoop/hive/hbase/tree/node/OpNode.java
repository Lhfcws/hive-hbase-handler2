package org.apache.hadoop.hive.hbase.tree.node;


import org.apache.hadoop.hive.hbase.tree.SargableParser;
import org.apache.hadoop.hive.hbase.tree.TreeUtil;

/**
 * Internal node is an opNode, while an opNode may not be a internal node.
 * @author lhfcws
 * @since 16/7/18
 */
public class OpNode extends Node {

    protected String operator;
    // 当前操作是否需要扫全表
    protected boolean scanAllTable = true;
    // 是否 and / or 这类逻辑运算符
    protected boolean isLogicOp = false;
    // 是否 not
    protected boolean isNot = false;
    protected transient SargableParser sargableParser = SargableParser.INSTANCE;

    public OpNode(String expression, SargableParser sargableParser) {
        super(expression);
        this.sargableParser = sargableParser;
    }

    public OpNode(String expression) {
        super(expression);
    }

    public OpNode() {
    }

    public SargableParser getSargableParser() {
        return sargableParser;
    }

    public void setSargableParser(SargableParser sargableParser) {
        this.sargableParser = sargableParser;
    }

    /**
     * 通过检查其儿子节点来判断当前节点的表达式是否需要扫全表
     *
     * @return
     */
    public boolean checkNeedScanAllTable() {
        if (operator == null || operator.isEmpty())
            return scanAllTable = true;

        if ("and".equals(operator.toLowerCase())) {
            boolean scan = true;
            for (Node n : getChildren()) {
                if (n instanceof OpNode) {
                    scan &= ((OpNode) n).isScanAllTable();
                }
            }
            return scanAllTable = scan;
        } else if (getSargableParser().isSargableOp(getOperator())) {
            boolean scan = true;
            boolean hasOpChildren = false;
            for (Node n : getChildren()) {
                if (n instanceof OpNode) {
                    hasOpChildren = true;
                    scan &= ((OpNode) n).isScanAllTable();
                }
            }
            if (!hasOpChildren) scan = false;
            return scanAllTable = scan;
        } else if (getSargableParser().isLogicOp(getOperator()) || TreeUtil.ROOT_NAME.equals(getOperator())) {
            boolean scan = false;
            for (Node n : getChildren()) {
                if (n instanceof OpNode) {
                    scan |= ((OpNode) n).isScanAllTable();
                }
            }
            return scanAllTable = scan;
        } else
            return scanAllTable = true;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public boolean isScanAllTable() {
        return scanAllTable;
    }

    public void setScanAllTable(boolean scanAllTable) {
        this.scanAllTable = scanAllTable;
    }

    public boolean isLogicOp() {
        return isLogicOp;
    }

    public void setLogicOp(boolean logicOp) {
        isLogicOp = logicOp;
    }

    @Override
    public String toString() {
        return "OpNode{" + super.expression +
                ", operator='" + operator + '\'' +
                ", scanAllTable=" + scanAllTable +
                ", isLogicOp=" + isLogicOp +
                '}';
    }

    public static OpNode createRootNode() {
        OpNode opNode = new OpNode();
        opNode.setOperator(TreeUtil.ROOT_NAME);
        opNode.setLogicOp(false);
        return opNode;
    }
}
