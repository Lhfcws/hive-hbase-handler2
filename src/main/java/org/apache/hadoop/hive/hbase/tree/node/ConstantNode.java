package org.apache.hadoop.hive.hbase.tree.node;

/**
 * @author lhfcws
 * @since 16/7/18
 */
public class ConstantNode extends Node {
    protected Object value;
    protected String valueType;

    public ConstantNode(String expression) {
        super(expression);
    }

    public ConstantNode() {
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    @Override
    public String toString() {
        return "ConstantNode{" + super.toString() +
                ", value=" + value +
                ", valueType='" + valueType + '\'' +
                '}';
    }
}
