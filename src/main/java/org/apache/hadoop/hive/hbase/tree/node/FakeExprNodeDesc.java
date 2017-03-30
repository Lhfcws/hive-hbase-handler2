package org.apache.hadoop.hive.hbase.tree.node;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.ArrayList;
import java.util.List;

/**
 * For test and fake data.
 * @author lhfcws
 * @since 16/7/18
 */
public class FakeExprNodeDesc extends ExprNodeDesc {
    public static final String GENERICFUNC = "org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc";
    public static final String COL = "org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc";
    public static final String CONSTANT = "org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc";

    protected String name = GENERICFUNC;
    protected String typeString = "boolean";
    protected List<ExprNodeDesc> children = new ArrayList<>();
    protected String exprString;
    protected List<String> cols = new ArrayList<>();

    public FakeExprNodeDesc(String name, String typeString, String exprString) {
        this.name = name;
        this.typeString = typeString;
        this.exprString = exprString;

        if (name.equals(COL)) {
            cols.add(exprString);
        }
    }

    public FakeExprNodeDesc(String name, String exprString) {
        this.name = name;
        this.exprString = exprString;

        if (name.equals(COL)) {
            cols.add(exprString);
        }
    }

    public FakeExprNodeDesc(String exprString) {
        this.exprString = exprString;
    }

    public FakeExprNodeDesc addChild(ExprNodeDesc... exprNodeDescs) {
        for (ExprNodeDesc exprNodeDesc : exprNodeDescs)
            this.children.add(exprNodeDesc);
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTypeString(String typeString) {
        this.typeString = typeString;
    }

    public void setChildren(List<ExprNodeDesc> children) {
        this.children = children;
    }

    public void setExprString(String exprString) {
        this.exprString = exprString;
    }

    public void setCols(List<String> cols) {
        this.cols = cols;
    }

    public String getExprString() {
        return exprString;
    }

    public String getTypeString() {
        return this.typeString;
    }

    public List<String> getCols() {
        return cols;
    }

    public List<ExprNodeDesc> getChildren() {
        return children;
    }

    public String getName() {
        return name;
    }

    @Override
    public ExprNodeDesc clone() {
        return null;
    }

    @Override
    public boolean isSame(Object o) {
        if (o != null && o instanceof FakeExprNodeDesc) {
            return getExprString().equals(((FakeExprNodeDesc) o).getExprString());
        } else
            return false;
    }
}
