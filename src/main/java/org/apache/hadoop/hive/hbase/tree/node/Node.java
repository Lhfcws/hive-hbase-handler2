package org.apache.hadoop.hive.hbase.tree.node;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract node.
 * @author lhfcws
 * @since 16/7/18
 */
public abstract class Node implements Serializable {
    protected String expression;
    // transient in case of cycle ref
    protected transient List<Node> children = new LinkedList<>();

    public Node(String expression) {
        this.expression = expression;
    }

    public Node() {
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }

    public Node addChild(Node wn) {
        this.children.add(wn);
        return this;
    }

    public boolean hasChildren() {
        return !this.children.isEmpty();
    }

    @Override
    public String toString() {
        return "expression=" + expression;
    }

    /**
     * print the whole tree nodes by using DFS recursion.
     * @return
     */
    public String treeToString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=================== WhereTree ==================");
        _treeToString(sb, this, 0);
        sb.append("\n================END WhereTree END===============");
        return sb.toString();
    }

    protected static void _treeToString(StringBuilder sb, Node now, int dep) {
        sb.append("\n");
        for (int i = 0; i < dep; i++)
            sb.append("+");
        sb.append(" ").append(now.toString());
        if (now.hasChildren()) {
            for (Node wn : now.getChildren())
                _treeToString(sb, wn, dep + 1);
        }
    }
}
