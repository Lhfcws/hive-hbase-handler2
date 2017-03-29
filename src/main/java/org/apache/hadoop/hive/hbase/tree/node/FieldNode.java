package org.apache.hadoop.hive.hbase.tree.node;


/**
 * @author lhfcws
 * @since 16/7/18
 */
public class FieldNode extends Node {
    protected String field;
    protected String fieldType;

    public FieldNode(String expression) {
        super(expression);
    }

    public FieldNode() {
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    @Override
    public String toString() {
        return "FieldNode{" + super.toString() +
                ", field='" + field + '\'' +
                ", fieldType='" + fieldType + '\'' +
                '}';
    }
}
