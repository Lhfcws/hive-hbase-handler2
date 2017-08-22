package org.apache.hadoop.hive.hbase.tree;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.hbase.tree.node.ConstantNode;
import org.apache.hadoop.hive.hbase.tree.node.FieldNode;
import org.apache.hadoop.hive.hbase.tree.node.Node;
import org.apache.hadoop.hive.hbase.tree.node.OpNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Build where tree.
 *
 * @author lhfcws
 * @since 16/7/18
 */
public class HiveTreeBuilder {
    protected static Log log = LogFactory.getLog(HiveTreeBuilder.class);
    protected static SessionState.LogHelper console = new SessionState.LogHelper(log);

    protected SargableParser sargableParser;

    public HiveTreeBuilder(SargableParser sargableParser) {
        this.sargableParser = sargableParser;
    }

    public HiveTreeBuilder() {
        sargableParser = new SargableParser();
    }

    protected Map<String, String> mappingNames = new HashMap<>();

    public void setMappingNames(Map<String, String> mappingNames) {
        this.mappingNames = mappingNames;
    }

    private String getMappingField(String field) {
        String mappingField = mappingNames.get(field);
        if (mappingField == null)
            return field;
        else
            return mappingField;
    }

    public OpNode build(ExprNodeDesc exprNodeDesc) {
        OpNode root = OpNode.createRootNode();
        _build(root, exprNodeDesc);
        root.checkNeedScanAllTable();
        return root;
    }

    private void _build(Node nowParent, ExprNodeDesc hiveNode) {
        if (hiveNode.getChildren() == null || hiveNode.getChildren().isEmpty()) {
            if (hiveNode.getName().endsWith("ExprNodeColumnDesc") && !hiveNode.getCols().isEmpty()) {
                FieldNode node = new FieldNode(hiveNode.getExprString());
                node.setFieldType(hiveNode.getTypeString());
                String field = hiveNode.getCols().get(0);
                node.setField(getMappingField(field));
                nowParent.addChild(node);
            } else if (hiveNode.getName().endsWith("ExprNodeConstantDesc")) {
                ConstantNode node = new ConstantNode(hiveNode.getExprString());
                node.setValue(hiveNode.getExprString());
                node.setValueType(hiveNode.getTypeString());
                nowParent.addChild(node);
            } else {
                // unknown operations
                OpNode opNode = new OpNode(hiveNode.getExprString());
                opNode.setScanAllTable(true);
                opNode.setLogicOp(false);
                nowParent.addChild(opNode);
            }
        } else {
            OpNode opNode = new OpNode(hiveNode.getExprString());
//             for test
            List<String> childrenExprs = new ArrayList<>();
            for (ExprNodeDesc nodeDesc : hiveNode.getChildren()) {
                childrenExprs.add(nodeDesc.getExprString());
            }

            // findParentOp is usuallly for local test
//            String operator = findParentOp(opNode.getExpression(), childrenExprs);
            String operator = findOp(hiveNode);

            if (operator != null) {
                opNode.setOperator(operator);
                if (sargableParser.isLogicOp(operator)) {
                    opNode.setLogicOp(true);
                }
            }
            nowParent.addChild(opNode);

            if (!sargableParser.isLogicOp(operator) && !sargableParser.isSargableOp(operator)) {
                return;
            }

            for (ExprNodeDesc nodeDesc : hiveNode.getChildren()) {
                _build(opNode, nodeDesc);
            }

            // 最后检查当前节点是否需要扫全表
            boolean scanAllTable = opNode.checkNeedScanAllTable();
            if (!scanAllTable && sargableParser.reverseOp(operator) != null) {
                if (opNode.getChildren().size() == 2 && opNode.getChildren().get(0) instanceof ConstantNode) {
                    Node swap = opNode.getChildren().get(0);
                    opNode.getChildren().set(0, opNode.getChildren().get(1));
                    opNode.getChildren().set(1, swap);
                    opNode.setOperator(sargableParser.reverseOp(opNode.getOperator()));
                }
            }
        }
    }

    /**
     * For test: 从 "id > 1" 等这样的语句中抽取出操作符
     *
     * @param parentExpr
     * @param childrenExprs
     * @return
     */
    protected String findParentOp(String parentExpr, List<String> childrenExprs) {
        for (String childExpr : childrenExprs) {
            parentExpr = parentExpr.replace(childExpr, "");
        }

        // strip a bracket clause
        parentExpr = parentExpr.trim();
        while (parentExpr.startsWith("(") && parentExpr.endsWith(")"))
            parentExpr = parentExpr.substring(1, parentExpr.length() - 1).trim();

        // strip function name
        if (parentExpr.endsWith(")"))
            parentExpr = parentExpr.replaceAll("\\([ ,]*\\)", "").trim();

        if (parentExpr.toLowerCase().contains("between") && parentExpr.toLowerCase().contains("and")) {
            return "between";
        } else {
            parentExpr = findUdfOp(parentExpr);
            return sargableParser.cleanSynonymOp(parentExpr);
        }
    }

    /**
     *
     * @param exprNodeDesc
     * @return
     */
    protected String findOp(ExprNodeDesc exprNodeDesc) {
        String udfName = TreeUtil.getGenericUDFNameFromExprDesc(exprNodeDesc);
        if (udfName == null)
            return null;

        String[] arr = udfName.split("\\.");
        udfName = arr[arr.length - 1];
        String op = sargableParser.sargableOpUDFClassMapping.get(udfName);
        if (op == null)
            return udfName;
        else
            return sargableParser.cleanSynonymOp(op);
    }

    /**
     *
     * @return
     */
    protected String findUdfOp(String udfName) {
        String op = sargableParser.sargableOpUDFClassMapping.get(udfName);
        if (op == null)
            return udfName;
        else
            return op;
    }
}
